# WAL Design

This document describes the design considerations for a Write-Ahead Log (WAL)
suitable for use with [`hashicorp/raft`](https://github.com/hashicorp/raft).

Specifically the library provides and instance of raft's `LogStore` and also
`StableStore` interfaces for storing both raft logs and the other small items
that require stable storage (like which term we voted in).

The advantage of this library over the `raft-boltdb` one that has been used
typically are:
 1. Efficient truncations that don't cause later appends to slow down due to
    freespace tracking issues in BoltDB's btree.
 2. More efficient appends due to only one fsync per append vs two in BoltDB.
 3. More efficient and suitable on-disk structure for a log vs a copy-on-write
    BTree.

We aim to provide roughly equivalent resiliency to crashes as respected storage
systems such as SQLite, LevelDB/RocksDB and etcd. BoltDB technically has a
stronger property due to it's page-aligned model (no partial sector overwrites).
We initially [designed a WAL on the same principals](/01-WAL-pages.md),
however felt that the additional complexity it adds wasn't justified given the
weaker assumptions that many other battle-tested systems above use.

Our design goals for crash recovery are:

 - Crashes at any point must not loose committed log entries or result in a
   corrupt file, even if in-flight sector writes are not atomic.
 - We _do_ assume [Powersafe Overwrites](#powersafe-overwrites-psow) where
   partial sectors can be appended to without corrupting existing data even in a
   power failure.
 - Latent errors (i.e. silent corruption in the FS or disk) _may_ be detected
   during a read, but we assume that the file-system and disk are responsible
   for this really. (i.e. we don't validate checksums on every record read).
   This is equivalent to SQLite, LMDB, BoltDB etc.

See the [system assumptions](#system-assumptions) and [crash
safety](#crash-safety) sections for more details.

## Limitations

Here are some notable limitations of this design.

 * Segment files can't be larger than 4GiB.
 * Individual records can't be larger than 4GiB.
 * Appended log entries have monotonically increasing `Index` fields with no
   gaps.
 * Only head or tail truncations are needed. `DeleteRange` will error if it's
   not a prefix of suffix of the log. `hashicorp/raft` never needs that.
 * No encryption or compression support.
   * Though we do provide a pluggable entry codec and internally treat each
     entry as opaque bytes so it's possible to supply a custom codec that
     transforms entries in any way desired.
 * For now segments are a fixed size and pre-allocated.

## Storage Format Overview

The WAL has two types of file: a meta store and one or more log segments.

All integers are encoded in little-endian byte order on disk.

### Meta Store

We need to provide a `StableStore` interface for small amounts of Raft data. We
also need to store some meta data about log segments to simplify managing them
in an atomic and crash-safe way.

Since this data is _generally_ small we could invent our own storage format with
some sort of double-buffering and limit ourselves to a single page of data etc.
But since performance is not critical for meta-data operations and size is
extremely unlikely to get larger than a few KiB, we choose instead the pragmatic
approach of using BoltDB for our `meta.db`.

The meta database contains two buckets: `stable` containing key/values persisted
by Raft via the `StableStore` interface, and `segments` which contains the
source-of-truth meta data about which segment files should be considered part of
the current log.

The `segments` bucket contains one record per segment file. The keys are just
the file names (which sort lexicographically). The values are JSON encoded
objects described by the following struct. JSON encoding is used as this is not
performance sensitive and it's simpler to work with and more human readable.

```go
type segmentInfo struct {
  ID         uint64
  BaseIndex  uint64
  MinIndex   uint64
  MaxIndex   uint64
  Codec      uint64
  BlockSize  uint32
  NumBlocks  uint32
  CreateTime time.Time
  SealTime   time.Time
}
```

The last segment (with highest key) is the "tail" and must be the only one where
`SealTime = 0`.

BoltDB's major performance issue currently is when large amounts of logs are
written and then truncated, the overhead of tracking all the freespace in the
file makes further appends slower as well as the file never shrinking again
(though disk space is rarely a real bottleneck).

Our use here is orders of magnitude lighter. Even if we allow 100GiB of logs to
be kept around, that is at least an order of magnitude larger than the largest
current known Consul user's worst-case log size, and two orders of magnitude
more than the largest Consul deployments steady-state. Assuming fixed 64MiB
segments, that would require about 1600 segments which encode to about 125 bytes
in JSON each. Even at this extreme, the meta DB only has to hold under 200KiB.

Even if a truncation occurs that reduces that all the way back to a single
segment, 200KiB is only a hundred or so pages (allowing for btree overhead) so
the freelist will never be larger than a single 4KB page.

### Segment Files

Segment files are pre-allocated (best effort) on creation to a fixed size, by
default we use 64MiB segment files.

Segment file format is inspired [by
RocksDB](https://github.com/facebook/rocksdb/wiki/Write-Ahead-Log-File-Format).

The file starts with a fixed-size header that is written once on creation.

```
0      1      2      3      4      5      6      7      8
+------+------+------+------+------+------+------+------+
| Magic                     | Reserved           | Vsn  |
+------+------+------+------+------+------+------+------+
| BlockSize                 | NumBlocks                 |
+------+------+------+------+------+------+------+------+
| BaseIndex                                             |
+------+------+------+------+------+------+------+------+
| SegmentID                                             |
+------+------+------+------+------+------+------+------+
```

| Field        | Type      | Description |
| ------------ | --------- | ----------- |
| `Magic`      | `uint32`  | The randomly chosen value `0x58eb6b0d`. |
| `Reserved`   | `[3]byte` | Bytes reserved for future file flags. |
| `Vsn`        | `uint8`   | The version of the file, currently `0x0`. |
| `BlockSize`  | `uint32`  | The size in bytes for each indexed block. |
| `NumBlocks`  | `uint32`  | The number of blocks preallocated in this segment file. |
| `BaseIndex`  | `uint64`  | The raft Index of the first entry that will be stored in this file. |
| `SegmentID`  | `uint64`  | A unique identifier for this segment file. |

Each segment file is named `<BaseIndex>-<SegmentID>.wal`. `BaseIndex` is
formatted in decimal with leading zeros and a fixed width of 20 chars.
`SegmentID` is formatted in lower-case hex with zero padding to 16 chars wide.
File names are also used as meta DB keys so need to sort lexicographically.

### Blocks

Log entries are appended to the file following the header. The last 24 bytes of
each `BlockSize` (1MiB default) of the file contains a special record called the
`BlockTrailer`.

This trailer enables:
 1. Incrementally writing out index as we go so recovery only has to scan the
    last block of the file.
 2. Allows for [binary search for specific entries](#log-lookup-by-index) in
    completed segments.

The Block Trailer format is as follows:

```
0      1      2      3      4      5      6      7      8
+------+------+------+------+------+------+------+------+
| FirstIndex                                            |
+------+------+------+------+------+------+------+------+
| BatchStart                | IndexStart                |
+------+------+------+------+------+------+------+------+
| NumEntries                | CRC                       |
+------+------+------+------+------+------+------+------+
```

| Field         | Type        | Description |
| ------------- | ----------- | ----------- |
| `FirstIndex`  | `uint64`    | The raft index of the first entry that starts in this block. In the special case of a whole block taken up by a single non-first fragment of an entry larger than `BlockSize` this contains the index of that entry. This case is distinguishable because `NumEntries` will be zero. |
| `BatchStart`  | `uint32`    | The _file_ offset of the first frame in the current append batch. Used to validate checksum up to the end of this block. Note this is a file offset as the batch may span many blocks. |
| `IndexStart`  | `uint32`    | The _file_ offset of the index frame for the block. May be zero if [there is no index frame](#index-frame). |
| `NumEntries`  | `uint32`    | The number of log entries whose _first_ frame was in this block. Note this could be zero in case of a block with only `Middle` or `Last` frames. |
| `CRC`         | `uint32`    | Castagnoli CRC32 of all frames (including headers and padding) from the larger of `BatchStart` offset or the start of the block until now. That includes the index frame and all empty space after it before the trailer. Also includes a CRC over the first 20 bytes of this trailer too. |

When appending to blocks, we take care to leave enough space for both the
fixed-length trailer, and a proceeding index frame. The index frame is written
out as soon as there is no more room in the block for the current index and
trailer, the indexes frame header plus an extra index entry, frame header and at
least 128 bytes for the new frame (since wasting a few bytes is probably better
than having to piece together fragments with just a few bytes in).

So the last frame before the trailer in each complete block will be type
`Index`, detailed below.

#### Block CRC

The last 4 bytes of the block trailer are the CRC used to detect torn writes.
The CRC is only calculated over bytes in the block that were written during the
same append batch that writes the trailer itself.

```
Case: BatchStart in same block

BlkStart    vBatchStart
+----...----+------------+-------------+----+---------+-----+
|           |h| entry  |0|h| index     |0000| trailer | CRC |
+----...----+------------+-------------+----+---------+-----+
            ^-----------------------------------------^
                  CRC calculated over these bytes
```

If a single append batch adds just one frame to the block before it's full and
then adds the index and trailer, `BatchStart` will point to the offset of that
one frame and the CRC will be calculated over only that one frame (and its
alignment padding), the index frame, the zeroes after the index frame before the
trailer and the bytes of the trailer except for the last 4.

```
Case: BatchStart in a previous block

PrevBlocks   vBatchStart    BlkStart
+-------...--+------...-----+------...------+-------------+----+---------+-----+
|            |              | record frames |h| index     |0000| trailer | CRC |
+-------...--+------...-----+------...------+-------------+----+---------+-----+
                            ^--------------------------------------------^
                                   CRC calculated over these bytes
```

If `BatchStart` was in some previous block it means the whole of this block was
filled as part of the batch. Calculate the CRC over all bytes in the block up to
the last 4. This can be done in a streaming fashion say 64 KiB at a time to
avoid reading the entire block into memory thanks to CRCs rolling property.

### Frames

Actual log entries are stored in frames. Each frame starts with a fixed-length
header.

```
0      1      2      3      4      5      6      7      8
+------+------+------+------+------+------+------+------+
| Type | FrameLen           | EntryLen/CRC              |
+------+------+------+------+------+------+------+------+
```

| Field         | Type        | Description |
| ------------- | ----------- | ----------- |
| `Type`        | `uint8`     | The frame type. See below. |
| `FrameLen`    | `uint24`    | The length (not including this header) of the payload of this frame. Note this is just 3 bytes since the largest a single frame can be is less than `BlockSize` and we don't anticipate supporting `BlockSize > 16MiB`. |
| `EntryLen`    | `uint32`    | For `First` fragments this contains the total length of the fragmented record to allow allocating enough space to read it up front. |
| `CRC`         | `uint32`    | For `Commit` frames, the Rolling Castagnoli CRC32 of all data appended in the current batch and block. See [Commit CRC](#commit-crc). |


| Type | Value | Description |
| ---- | ----- | ----------- |
| `Full`    | `0x0` | The frame contains an entire log entry. |
| `First`   | `0x1` | The frame contains the first fragment of a longer log entry. |
| `Middle`  | `0x2` | The frame contains a middle fragment of a longer log entry. |
| `Last`    | `0x3` | The frame contains a last fragment of a longer log entry. |
| `Index`   | `0x4` | The frame contains a block index array, not actual log entries. |
| `Commit`  | `0x5` | The frame contains a CRC for all data written in a batch. |

Note that `Commit` frames are a little different. They have no payload so
`FrameLen` is always 0.

#### Index Frame

An index frame payload is an array of uint32 file offsets for the records. The
first element of the array contains the file offset of the frame containing the
first fragment of the entry with index `FirstIndex` in the block trailer. This
might not be the first frame in the block since the block might begin with
either the file header (for the first block) or a `Last` frame of a previous
record.

In cases where records are larger than whole blocks, at least one block will
contain only a single `Middle` frame. In this case it will have no index record
since there are zero entries that begin in this block. `IndexStart` will be `0`.
This is safe because `IndexStart` is always a file offset and files always start
with a file header so no valid index can start there.

#### Commit CRC

The CRC stored in a commit frame header is calculated over all bytes written to
the same block in the same batch.

If the batch started appending in the same block, this will be all bytes from
immediately after the previous commit frame up to the commit frame header start
(but not including it).

If the batch started appending in a previous block then this will be all bytes
from the block start to the commit frame.

#### Alignment

All frame headers are written with 8-byte alignment to ensure they remain in a
single disk sector. We don't entirely depend on atomic sector writes for
correctness, but it's a simple way to improve our chances or being able to read
through the file on a recovery.

We add an implicit 0-7 null bytes after each frame to ensure the next frame
header is aligned. This padding is _not_ represented in `FrameLen` but it is
always present and is deterministic by rounding up `FrameLen` to the nearest
multiple of 8. It is always accounted for when reading and CRCs are calculated
over raw bytes written so always include the padding bytes.

Despite alignment we still don't blindly trust the headers we read are valid.
The CRC check or invalid record format detect torn writes in the last batch
written and we always safety check the size of lengths read before allocating
them - frame lengths can't be bigger than `BlockSize`, file offsets can't be
beyond the actual file size, and `EntryLen` can't be bigger than the space left
in the segment file.

## Log Lookup by Index

Fixed-size blocks and pre-allocated segment files allow us to efficiently find a
record by index (or similarly, the tail of the log) by binary search.

We search for the block containing index K as follows:
 1. Locate the correct segment (highest `BaseIndex` <= K) or return
    `NotFound`
 2. If the segment is the tail segment and the record is in the current partial
    block being written, return it directly from memory.
 3. Binary search the blocks in the file (i.e. starting at the middle). We use
    `sort.Search` which finds the smallest block for which the func returns
    true. We need this to be the block that contains K or `NumBlocks` if not
    found. For each block we read the fixed-size trailer at the end.
    1. If the trailer is all zeros, assume we are in the tail and the record is
       in a lower block, return false.
    2. If the trailer is non-zero but corrupt return `Corrupt` error
    3. Otherwise return `Trailer.FirstIndex <= K`

This finds the block where the entry `K` starts (or `NumBlocks`).

Once we have the block, we can read the correct entry using the block index:
 1. Read the relevant part of the index frame:
    1. The `K` entry offset in the block (`blockOffset`) will be at `K` -
       `FirstIndex`.
    2. The first frame offset will be in in the index frame at `IndexStart` +
       `FrameHdrSize` + `blockOffset`. Read a `uint32` value at that offset as
       `frameStart`.
    3. If `blockOffset < NumEntries-1` then also read the next frame offset into
       `frameEnd`. If we just read the last offset, then use `frameEnd := IndexStart`.
    4. Allocate a buffer big enough to read `frameEnd - frameStart` bytes
    6. Read `frameStart` to `frameEnd` bytes into buffer and parse frame.
       1. If a `Full` record return relevant slice minus header and padding.
       2. If a `First` record, read the `EntryLen` and if needed re-allocate
          buffer to fit the whole entry. Then continue reading frames from
          block, wrapping over to next block(s) if necessary.

Big-O performance is O(log N). We expect performance of lookups to be adequate
on an SSD without further optimization. Especially given that Raft users
typically wrap the `LogStore` in `raft.LogCache` that caches the most recent N
entries in memory anyway and almost all accesses in steady state are for the
most recent entries during replication.

# Crash Safety

Crash safety must be maintained through three type of write operation: appending
a batch of entries, truncating from the head (oldest) entries, and truncating
the newest entries.

## Appending Entries

We want to `fsync` only once for an append batch, however many entries were in
it. We assume [Powersafe Overwrites](#powersafe-overwrites-psow) or PSOW, a
weaker assumption than atomic sector writes in general. Thanks to PSOW, we
assume we can start appending at the tail of the file right after previously
committed entries even if the new entries will be written to the same sector as
the older entries, and that the system will never corrupt the already committed
part of the sector even if it is not atomic and arbitrarily garbles the part of
the sector we actually did write.

At the end of each block we append a trailer with a checksum over the data
written to that block _during this batch_. The CRC might be over the whole block
if this batch has spanned multiple blocks but will commonly be only the suffix
of the block that we appended in this batch. See [Block CRC](#block-crc).

At the end of the batch we write a `Commit` frame containing the
[CRC](#commit-crc) over the data in that partial block from the current batch.

In a crash one of the following states occurs:
 1. All sectors modified across all blocks make it to disk (crash _after_ fsync).
 2. A torn write: one or more sectors, anywhere in the modified tail of the file
    might not be persisted. We don't assume they are zero unlike some code, they
    might be arbitrarily garbled (crash _before_ fsync).

We can check which one of these is true with the recovery procedure outlined
below. If we find the last batch _was_ torn. It must not have been acknowledged
to Raft yet (since `fsync` can't have returned) and so it is safe to assume that
the previous commit frame is the tail of the log we've actually acknowledged.

### Recovery

We cover recovering the segments generally below since we have to account for
truncations. All segments except the tail were fsynced before the new tail was
added to the meta DB so we can assume they are all intact.

On startup we just need to recover the tail log as follows:

 1. If the file doesn't exist, create it from Meta DB information. DONE.
 2. Open file and validate header matches filename. If not delete it and go to 1.
 3. Search for the last complete block. We can use binary search similar to [log
    lookup](#log-lookup-by-index).
 4. If there is a complete block found:
    1. [validate it's CRC](#block-crc) to ensure that all data written to that
    block in the last append batch is intact.
    2. If the `BatchStart` on the last block trailer is in a previous block,
       validate the CRCs of all blocks from there to the last complete block
       too.
    3. If any block CRCs don't match, treat `BatchStart` as if it's the end of
       the file and restart recovery from 3.
 5. Read through the final partial block frame-by-frame, rebuilding an in-memory
    index for the frame offsets.
 6. When we run out of frames (all zero, or invalid header, or index frame, or
    end of block)
    1. Check the last frame is a `Commit` frame, if not ROLLBACK.
    2. Validate the CRC in commit frame from start of block or previous commit frame in block. If it fails ROLLBACK.
    3. If it's valid, we found the tail and last batch is intact. DONE.
 7. If we need to ROLLBACK, find the previous commit frame from the last frame
    we read.
    1. If there is no previous commit frame in the partial block, `BatchStart`
       from the last block trailer becomes our new effective tail. Go back to
       the start of the block that's in and read up to that point. We don't need
       to validate checksums because we know all data up to `BatchStart` was
       committed by `fsync` in the previous batch so can't be torn. DONE.

## Head Truncations

The most common form of truncation is a "head" truncation or removing the oldest
prefix of entries after a periodic snapshot has been made to reclaim space.

To be crash safe we can't rely on atomically updating or deleting multiple
segment files. The process looks like this.

 1. In one transaction on Meta DB:
    1. Update the `meta.min_index` to be the new min.
    2. Delete any segments from the `segments` bucket that are sealed and where
       their highest index is less than the new min index.
    3. Commit Txn. This is the commit point for crash recovery.
 2. Update in memory segment state to match (if not done already with a lock
    held).
 3. Delete any segment files we just removed from the meta DB.

### Recovery

The meta data update is crash safe thanks to BoltDB being the source of truth.

 1. Reload meta state from Meta DB.
 2. Walk the files in the dir.
 2. For each one:
    1. Check if that file is present in Meta DB. If not mark it for deletion.
    2. (optionally) validate the file header file size and final block trailer
       to ensure the file appears to be well-formed and contain the expected
       data.
 4. Delete the obsolete segments marked (could be done in a background thread).

 ## Tail Truncations

 Raft occasionally needs to truncate entries from the tail of the log, i.e.
 remove the _most recent_ N entries. This can occur when a follower has
 replicated entries from an old leader that was partitioned with it, but later
 discovers they conflict with entries committed by the new leader in a later
 term. The bounds on how long a partitioned leader can continue to replicate to
 a follower are generally pretty small (30 seconds or so) so it's unlikely that
 the number of records to be truncated will ever be large compared to the size
 of a segment file, but we have to account for needing to delete one or more
 segment files from the tail, as well as truncate older entries out of the new
 tail.

 This follows roughly the same pattern as head-truncation, although there is an
 added complication. A naive implementation that used only the baseIndex as a
 segment file name could in theory get into a tricky state where it's ambiguous
 whether the tail segment is an old one that was logically truncated away but we
 crashed before actually unlinking, or a new replacement with committed data in.

 It's possible to solve this with complex transactional semantics but we take
 the simpler approach of just assigning every segment a unique identifier
 separate from it's baseIndex. So to truncate the tail follows the same
 procedure as the head above: segments we remove from Meta DB can be
 un-ambiguously deleted on recovery because their IDs won't match even if later
 segments end up with the same baseIndex.

 Since these truncations are generally rare and disk space is generally not a
 major bottleneck, we also choose not to try to actually re-use a segment file
 that was previously written and sealed by truncating it etc. Instead we just
 mark it as "sealed" in the Meta DB and with a MaxIndex of the highest index
 left after the truncation (which we check on reads) and start a new segment at
 the next index.

## System Assumptions

There are no straight answers to any question about which guarantees can be
reliably relied on across operating systems, file systems, raid controllers and
hardware devices. We state [our assumptions](#our-assumptions) followed by a
summary of the assumptions made by some other respectable sources for
comparison.

### Our Assumptions

We've tried to make the weakest assumptions we can while still keeping things
relatively simple and performant.

We assume:
 1. That while silent latent errors are possible, they are generally rare and
    there's not a whole lot we can do other than return a `Corrupt` error on
    read. In most cases the hardware or filesystem will detect and return an
    error on read anyway for latent corruption not doing so could be regarded as
    a bug in the OS/filesystem/hardware. For this reason we don't go out of our
    way to checksum everything to protect against "bitrot", but where checksums
    exist and we know the corruption can't have been due to a torn write, we
    return an error to users. This is roughly equivalent to assumptions in
    BoltDB, LMDB and SQLite.

    While we respect the work in [Protocol Aware Recovery for Consensus-based
    Storage](https://www.usenix.org/system/files/conference/fast18/fast18-alagappan.pdf)
    we choose not to implement a WAL format that allows identifying the index
    and term of "lost" records on read errors so they can be recovered from
    peers. This is mostly for the pragmatic reason that the Raft library this is
    designed to work with would need a major re-write to take advantage of that
    anyway. The proposed format in that paper also seems to make stronger
    assumptions about sector atomicity than we are comfortable with too.
 2. That sector writes are _not_ atomic. (Equivalent to SQLite, weaker than
    almost everything else).
 3. That writing a partial sector does _not_ corrupt any already stored data in
    that sector outside of the range being written (
    [PSOW](#powersafe-overwrites-psow)), (Equivalent to SQLite's defaults,
    RocksDB and Etcd).
 3. That `fsync` as implemented in Go's standard library actually flushes all
    written sectors of the file to persistent media.
 4. That `fsync` on a parent dir is sufficient to ensure newly created files are
    not lost after a crash (assuming the file itself was written and `fsync`ed
    first).
 6. That appending to files may not be atomic since the filesystem metadata
    about the size of the file may not be updated atomically with the data.
    Generally we pre-allocate files where possible without writing all zeros but
    we do potentially extend them if the last batch doesn't fit into the
    allocated space or the filesystem doesn't support pre-allocation. Either way
    we don't rely on the filesystem's reported size and validate the tail is
    coherent on recovery.

### Published Paper on Consensus Disk Recovery

In the paper on [Protocol Aware Recovery for Consensus-based
Storage](https://www.usenix.org/system/files/conference/fast18/fast18-alagappan.pdf)
the authors assume that corruptions of the log can happen due to either torn
writes (for multi-sector appends) or latent corruptions after commit. They
explain the need to detect which it was because torn writes only loose
un-acknowledged records and so are safe to detect and truncate, while corruption
of previously committed records impacts the correctness of the protocol more
generally. Their whole paper seems to indicate that these post-commit
corruptions are a major problem that needs to be correctly handled (which may
well be true). On the flip side, their WAL format design writes a separate index
and log, and explicitly assumes that because the index entries are smaller than
a 512 sector size, that those are safe from corruption during a write.

The core assumptions here are:
  1. Latent, silent corruption of committed data needs to be detected at
     application layer with a checksum per record checked on every read.
  2. Sector writes are atomic.
  3. Sector writes have [powersafe overwrites](#powersafe-overwrites-psow).

### SQLite

The SQLite authors have a [detailed explanation of their system
assumptions](https://www.sqlite.org/atomiccommit.html) which impact correctness
of atomic database commits.

> SQLite assumes that the detection and/or correction of bit errors caused by cosmic rays, thermal noise, quantum fluctuations, device driver bugs, or other mechanisms, is the responsibility of the underlying hardware and operating system. SQLite does not add any redundancy to the database file for the purpose of detecting corruption or I/O errors. SQLite assumes that the data it reads is exactly the same data that it previously wrote.

Is very different from the above paper authors whose main point of their paper
is predicated on how to recover from silent corruptions of the file caused by
hardware, firmware or filesystem errors on read.

Note that this is a pragmatic position rather than a naive one: the authors are
certainly aware that file-systems have bugs, that faulty raid controllers exist
and even that hardware anomalies like high-flying or poorly tracking disk heads
can happen but choose _not_ to protect against that _at all_. See their
[briefing for linux kernel
developers](https://sqlite.org/lpc2019/doc/trunk/briefing.md) for more details
on the uncertainty they understand exists around these areas.

> SQLite has traditionally assumed that a sector write is not atomic.

These statements are on a page with this disclaimer:

> The information in this article applies only when SQLite is operating in "rollback mode", or in other words when SQLite is not using a write-ahead log.

[WAL mode](https://sqlite.org/wal.html) docs are less explicit on assumptions
and how crash recovery is achieved but we can infer some things from the [file
format](https://sqlite.org/fileformat2.html#walformat) and
[code](https://github.com/sqlite/sqlite/blob/master/src/wal.c) though.

> The WAL header is 32 bytes in size...

> Immediately following the wal-header are zero or more frames. Each frame consists of a 24-byte frame-header followed by a page-size bytes of page data.

So each dirty page is appended with a 24 byte header making it _not_ sector
aligned even though pages must be a multiple of sector size.

Commit frames are also appended in the same way (and fsync called if enabled as
an option). If fsync is enabled though (and POWERSAFE_OVERWRITE disabled),
SQLite will "pad" to the next sector boundary (or beyond) by repeating the last
frame until it's passed that boundary. For some reason, they take great care to
write up to the sector boundary, sync then write the rest. I assume this is just
to avoid waiting to flush the redundant padding bytes past the end of the sector
they care about. Padding prevents the next append from potentially overwriting
the committed frame's sector.

But...

> By default, SQLite assumes that an operating system call to write a range of bytes will not damage or alter any bytes outside of that range even if a power loss or OS crash occurs during that write. We call this the "powersafe overwrite" property. Prior to version 3.7.9 (2011-11-01), SQLite did not assume powersafe overwrite. But with the standard sector size increasing from 512 to 4096 bytes on most disk drives, it has become necessary to assume powersafe overwrite in order to maintain historical performance levels and so powersafe overwrite is assumed by default in recent versions of SQLite.

> [assuming no power safe overwrite] In WAL mode, each transaction had to be padded out to the next 4096-byte boundary in the WAL file, rather than the next 512-byte boundary, resulting in thousands of extra bytes being written per transaction.

> SQLite never assumes that database page writes are atomic, regardless of the PSOW setting.(1) And hence SQLite is always able to automatically recover from torn pages induced by a crash. Enabling PSOW does not decrease SQLite's ability to recover from a torn page.

So they basically changed to make SSDs performant and now assume _by default_
that appending to a partial sector won't damage other data. The authors are
explicit that ["powersafe overwrite"](#powersafe-overwrites-psow) is a separate
property from atomicity and they still don't rely on sector atomicity. But they
do now assume powersafe overwrites by default.

To summarize, SQLite authors assume:
 1. Latent, silent corruptions of committed data should be caught by the file
    system or hardware and so should't need to be accounted for in application
    code.
 2. Sector writes are _not_ atomic, but...
 3. Partial sector overwrites can't corrupt committed data in same sector (by
    default).

### Etcd WAL

The authors of etcd's WAL similarly to the authors of the above paper indicate
the need to distinguish between torn writes and silent corruptions.

They maintain a rolling checksum of all records which is used on recovery only
which would imply they only care about torn writes since per-record checksums
are not checked on subsequent reads from the file after recovery. But they have
specific code to distinguish between torn writes and "other" corruption during
recovery.

They are careful to pad every record with 0 to 7 bytes such that the length
prefix for the next record is always 8-byte aligned and so can't span more than
one segment.

But their method of detecting a torn-write (rather than latent corruption)
relies on reading through every 512 byte aligned slice of the set of records
whose checksum has failed to match and seeing if there are any entirely zero
sectors.

This seems problematic in a purely logical way regardless of disk behavior: if a
legitimate record contains more than 1kb of zero bytes and happens to ever be
corrupted after writing, that record will be falsely detected as a torn-write
because at least one sector will be entirely zero bytes. In practice this
doesn't matter much because corruptions caused by anything other than torn
writes are likely very rare but it does make me wonder why bother trying to tell
the difference.

The implied assumptions in their design are:
 1. Latent, silent corruption needs to be detected on recovery, but not on every
    read.
 2. Sector writes are atomic.
 3. Partial sector writes don't corrupt existing data.
 3. Torn writes (caused by multi-segment appends) always leave sectors all-zero.

### LMDB

Symas' Lightning Memory-mapped Database or LMDB is another well-used and
respected DB file format (along with Go-native port BoltDB used by Consul,
etcd and others).

LMDB writes exclusively in whole 4kb pages. LMDB has a copy-on-write design
which reuses free pages and commits transactions using the a double-buffering
technique: writing the root alternately to the first and second pages of the
file. Individual pages do not have checksums and may be larger than the physical
sector size. Dirty pages are written out to new or un-used pages and then
`fsync`ed before the transaction commits so there is no reliance on atomic
sector writes for data pages (a crash might leave pages of a transaction
partially written but they are not linked into the tree root yet so are ignored
on recovery).

The transaction commits only after the double-buffered meta page is written
out. LMDB relies on the fact that the actual content of the meta page is small
enough to fit in a single sector to avoid "torn writes" on the meta page. (See
[the authors
comments](https://ayende.com/blog/162856/reviewing-lightning-memory-mapped-database-library-transactions-commits)
on this blog). Although sector writes are assumed to be atomic, there is no
reliance on partial sector writes due to the paged design.

The implied assumptions in this design are:
 1. Latent, silent corruptions of committed data should be caught by the file
    system or hardware and so should't need to be accounted for in application
    code.
 2. Sector writes _are_ atomic.
 3. No assumptions about Powersafe overwrite since all IO is in whole pages.


### BoltDB

BoltDB is a Go port of LMDB so inherits almost all of the same design
assumptions. One notable different is that the author added a checksum to
metadata page even though it still fits in a single sector. The author noted
in private correspondence that this was probably just a defensive measure
rather than a fix for a specific identified flaw in LMDB's design.

Initially this was _not_ used to revert to the alternate page on failure because
it was still assumed that meta fit in a single sector and that those writes were
atomic. But [a report of Docker causing corruption on a
crash](https://github.com/boltdb/bolt/issues/548) seemed to indicate that the
atomic sector writes assumption _was not safe_ alone and so the checksum was
used to detect non-atomic writes even on the less-than-a-sector meta page.

BoltDB is also an important base case for our WAL since it is used as the
current log store in use for many years within Consul and other HashiCorp
products.

The implied assumptions in this design are:
1. Latent, silent corruptions of committed data should be caught by the file
   system or hardware and so should't need to be accounted for in application
   code.
2. Sector writes are _not_ atomic.
3. No assumptions about Powersafe overwrite since all IO is in whole pages.


### RocksDB WAL

RocksDB is another well-respected storage library based on Google's LevelDB.
RocksDB's [WAL
Format](https://github.com/facebook/rocksdb/wiki/Write-Ahead-Log-File-Format)
uses blocks to allow skipping through files and over corrupt records (which
seems dangerous to me in general but perhaps they assume only torn-write
corruptions are possible?).

Records are packed into 32KiB blocks until they don't fit. Records that are
larger use first/middle/last flags (which inspired this library) to consume
multiple blocks.

RocksDB WAL uses pre-allocated files but also re-uses old files on a circular
buffer pattern since they have tight control of how much WAL is needed. This
means they might be overwriting old records in place.

Each record independently gets a header with a checksum to detect corruption or
incomplete writes, but no attempt is made to avoid sector boundaries or partial
block writes - the current block is just appended to for each write.

Implied assumptions:
 1. No Latent Corruptions? This isn't totally clear from the code or docs, but
    the docs indicate that a record with a mismatching checksum can simply be
    skipped over which would seem to violate basic durability properties for a
    database if they were already committed. That would imply that checksums
    only (correctly) detect torn writes with latent corruption not accounted
    for.
 2. Sector writes _are_ atomic.
 3. Partial sector writes don't corrupt existing data.

### Are Sector Writes Atomic?

Russ Cox asked this on twitter and tweeted a link to an [excellent Stack
Overflow
answer](https://stackoverflow.com/questions/2009063/are-disk-sector-writes-atomic)
about this by one of the authors of the NVME spec.

> TLDR; if you are in tight control of your whole stack from application all the way down the the physical disks (so you can control and qualify the whole lot) you can arrange to have what you need to make use of disk atomicity. If you're not in that situation or you're talking about the general case, you should not depend on sector writes being atomic.

Despite this, _most_ current best-of-breed database libraries (notably except
SQLite and potentially BoltDB), [many linux file
systems](https://lkml.org/lkml/2009/8/24/156), and all academic papers on disk
failure modes I've found so far _do_ assume that sector writes are atomic.

I assume that the authors of these file systems, databases and papers are not
unaware of the complexities described in the above link or the possibility of
non-atomic sector writes, but rather have chosen to put those outside of the
reasonable recoverable behavior of their systems. The actual chances of
encountering a non-atomic sector write in a typical, modern system appear to be
small enough that these authors consider that a reasonable assumption even when
it's not a guarantee that can be 100% relied upon. (Although the Docker bug
linked above for [BoltDB](#boltdb) seems to indicate a real-world case of this
happening in a modern environment.)

### Powersafe Overwrites (PSOW)

A more subtle property that is a weaker assumption that full sector atomicity is
termed by the [SQLite authors as "Powersafe
Overwrites"](https://www.sqlite.org/psow.html) abbreviated PSOW.

> By default, SQLite assumes that an operating system call to write a range of bytes will not damage or alter any bytes outside of that range even if a power loss or OS crash occurs during that write. We call this the "powersafe overwrite" property. Prior to version 3.7.9 (2011-11-01), SQLite did not assume powersafe overwrite. But with the standard sector size increasing from 512 to 4096 bytes on most disk drives, it has become necessary to assume powersafe overwrite in order to maintain historical performance levels and so powersafe overwrite is assumed by default in recent versions of SQLite.

Those who assume atomic sector writes _also_ assume this property but the
reverse need not be true. SQLite's authors in the page above assume nothing
about the atomicity of the actual data written to any sector still even when
POWERSAFE_OVERWRITE is enabled (which is now the default). They simply assume
that no _other_ data is harmed while performing a write that overlaps other
sectors, even if power fails.

It's our view that while there certainly can be cases where this assumption
doesn't hold, it's already weaker than the atomic sector write assumption that
most reliable storage software assumes today and so is safe to assume on for
this case.

### Are fsyncs reliable?

Even when you explicitly `fsync` a file after writing to it, some devices or
even whole operating systems (e.g. macOS) _don't actually flush to disk_ to
improve performance.

In our case, we assume that Go's `os.File.Sync()` method is makes the best
effort it can on all modern OSes. It does now at least behave correctly on macOS
(since Go 1.12). But we can't do anything about a lying hardware device.

# Future Extensions

 * **Auto-tuning segment size.** This format allows for segments to be different
   sizes. We could start with a smaller segment size of say a single 1MiB block
   and then measure how long it takes to fill each segment. If segments fill
   quicker than some target rate we could double the allocated size of the next
   segment. This could mean a burst of writes makes the segments grow and then
   the writes slow down but the log would take a long time to free disk space
   because the segments take so long to fill. Arguably not a terrible problem,
   but we could also have it auto tune segment size down when write rate drops
   too. The only major benefit here would be to allow trivial usages like tests
   not need a whole 64MiB of disk space to just record a handful of log entries.
   But those could also just manually configure a smaller segment size.

# References

In no particular order.

**Files and Crash Recovery**
* [Files are hard](https://danluu.com/file-consistency/)
* [Files are fraught with peril](https://danluu.com/deconstruct-files/)
* [Ensuring data reaches disk](https://lwn.net/Articles/457667/)
* [Write Atomicity and NVME Device Design](https://www.bswd.com/FMS12/FMS12-Rudoff.pdf)
* [Durability: NVME Disks](https://www.evanjones.ca/durability-nvme.html)
* [Intel SSD Durability](https://www.evanjones.ca/intel-ssd-durability.html)
* [Are Disk Sector Writes Atomic?](https://stackoverflow.com/questions/2009063/are-disk-sector-writes-atomic/61832882#61832882)
* [Protocol Aware Recovery for Consensus-based Storage](https://www.usenix.org/system/files/conference/fast18/fast18-alagappan.pdf)
* [Atomic Commit in SQLite](https://www.sqlite.org/atomiccommit.html)
* ["Powersafe Overwrites" in SQLite](https://www.sqlite.org/psow.html)
* [An Analysis of Data Corruption in the Storage Stack](https://www.cs.toronto.edu/~bianca/papers/fast08.pdf)

**DB Design and Storage File layout**
* [BoltDB Implementation](https://github.com/boltdb/bolt)
* LMDB Design: [slides](https://www.snia.org/sites/default/files/SDC15_presentations/database/HowardChu_The_Lighting_Memory_Database.pdf), [talk](https://www.youtube.com/watch?v=tEa5sAh-kVk)
* [SQLite file layout](https://www.sqlite.org/fileformat.html)

**WAL implementations**
* [SQLite WAL Mode](https://sqlite.org/wal.html)
* [RocksDB WAL Format](https://github.com/facebook/rocksdb/wiki/Write-Ahead-Log-File-Format)]
* [etcd implementation](https://github.com/etcd-io/etcd/tree/master/wal)