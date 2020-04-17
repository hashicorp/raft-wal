# raft-wal: Raft Log Stable Store Backend [EXPERIMENTAL]

A Raft `StableStore`/`LogStore` implementation that use log files with Write-Ahead-Log (WAL) techniques.

This is very experimental work.

It is aimed to be crash safe in all Raft supported environments and relatively
fast with predictable performance.

The implementation now is single-writer singler-reader.  Effectively no concurrency handling is involved.

Few features:
* crash safe
* log compression
* *coming* log storage encryption

### Internals

#### `raft.StableStore`

[`meta.go`](./meta.go) implements a basic key-value store, to implement `raft.StableStore`.  The store is designed for small values (must fit under 4Kb).

The implementation uses a double-buffering technique, established by [`LMDB`](https://www.snia.org/sites/default/files/SDC15_presentations/database/HowardChu_The_Lighting_Memory_Database.pdf) and followed by [`boltdb`](https://github.com/boltdb/bolt).

The key aspect of double-buffering is that the meta file contains two physical slots for storing data: one current and one unused.  On update, the write uses the unused slot and mark it with a higher version number; the commit occurs when the write is written successfully.  On restore, the two slots are compared and the slot with valid data and higher version number is chosen as the current on.  A checksum is used to identify torn writes.

#### `raft.LogStore`

The Log Store uses a series of files, called segments, that contain contigious
log entries. The log entries are written in sequence. The number of log entries
in each segment is configurable.

Each segment file contain a header with basic config info (e.g. base index,
compression type), followed by log entries. Once, log entries go beyond the
segment range, the segment is sealed and a final entry containing an index of
the log entry offsets is written. Once sealed, the file is never modified.
Before being sealed, the offset of active indexes lives in memory.

Generally, there will be one active segment, where write flows, and many sealed segments containing old indexes.

Range deletions are tricky, `raft-wal` supports only head or tail truncations (no mid-range):
* Head truncations are simple: an internal state of the first index is updated and then all segments prior to the index are deleted.
  * The first index state is stored in the `meta` storage referenced above for atomicity.
  * deletion commits when the first index state is committed.  Old segments are deleted immediately or on subsequent recoveries
  
* Tail truncations are multi-step processes that require transactions.
  * We write a truncateTail marker, truncate from active segment, or delete no-longer used segments and start a new segment file for the new base index, then remove the truncateTail marker.
  * the transaction commits when the truncateTail marker is cleared.
  * Old segment files are never modified, and may have ranges that overlap with the newly created segment file.  This is fine, the new segment file with higher higher base index will superceed the old segment.
  * tail truncations are expensive.  They require at least three fsync calls: for writing and cleaning the marker, and for the truncation action itself or the deletion and creation of new segment files.
  

#### Open Ideas And Next Steps

The following are known issues and things we should consider:

* Atomic directory creation: If system crashed while raft-log is being initialized but before any data is written, an operator may need to delete the directory.

* Optimize tail truncations: trail truncations are expensive requiring 3 fsyncs!  Explore options that reduces syncs to one
  * A possible consideration: if truncations require deleting segment files, create a new segment file with a epoch number in the name before deleting old files.  Once the segment file is created, the truncation commits.  On recovery, `raft-wal` can detect the truncation and proceed to delete the truncated range segments if crash occurs.

* Optimize read path - locking.  Currently, segment reads and writes share the same lock which is held during the IO operation.  Consider relaxing it.
  * One complexity: if we support in-place truncations for active segments (to avoid creating many files), we need to protect against concurrent deletions and reads.  Maybe always start a new segment file?!
  
* Optimize read path - caching and serialization.  The implementation now uses `read` IO call with little caching.  Profile and consider the following:
  * use `madvice` so OS caches files effectively.  Accesses should be mostly sequentials (sepcially in beginning)
  * investigate bytes copying and allocations in read path.
  * use `mmap` to achieve zero copy, like lmdb and botldb.  Watch out for https://medium.com/@valyala/mmap-in-go-considered-harmful-d92a25cb161d

* Optimize memory and allocations.  This implementation uses lots of bytes and allocations:
  * profile and eliminate as many allocations as possible
  * Consider using `sync.Pool` for managing byte buffers
  * consider `mmap` as stated above

* Implement encryption wrappers!  Let's encrypt log entries

* Segment files must be under 4GB, and will fail unexpectedly if `SegmentChunkSize` isn't set correctly
  * Log entries are generally under 256KB.  Log entries larger than 4MB can cause network and latency issues
  * This implementation segments purely on number of segments and all batched entries are persisted in the same ssegment file.
  * Generally, 4GB limit isn't a concern and should mean that `SegmentChunkSize` should be smaller.
  * consider documenting knobs and add a warning about file size or super large SegmentChunkSize values

* Review Checksum use:
  * the seal byte and index offset update happen in place without a checksum.  Should in a single sector, so hopefully atomic!
  * log entries are checksumed individually.  Etcd builds checksum based on history as well

* Test crash recovery with [ALICE](https://github.com/madthanu/alice).

## Bibliography

**Files and Crash Recovery***
* [Files are hard](https://danluu.com/file-consistency/)
* [Files are fraught with peril](https://danluu.com/deconstruct-files/)
* [Ensuring data reaches disk](https://lwn.net/Articles/457667/**

**DB Design and Storage File layout**
* [BoltDB Implementation](https://github.com/boltdb/bolt)
* LMVDB Design: [slides](https://www.snia.org/sites/default/files/SDC15_presentations/database/HowardChu_The_Lighting_Memory_Database.pdf), [talk](https://www.youtube.com/watch?v=tEa5sAh-kVk**.
* [SQLite file layout](https://www.sqlite.org/fileformat.html)
* [PostgreSQL directory and file layout](https://www.postgresql.org/docs/9.0/storage-file-layout.html)

**WAL implementations**
* [etcd implementation](https://github.com/etcd-io/etcd/tree/master/clientv3)
* [Jocko, Kafka implemented in Golang](https://github.com/travisjeffery/jocko/tree/master/commitlog)
* [Paul Banks's raft-wal](https://github.com/banks/raft-wal)
