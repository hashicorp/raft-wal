# ALICE Testing

This directory uses a tool called ALICE (Application-Level Intelligent Crash
Explorer) which was developed alongside the OSDI 14 paper titled [_All File
Systems Are Not Created Equal: On the Complexity of Crafting Crash-Consistent
Applications_](https://www.usenix.org/system/files/conference/osdi14/osdi14-paper-pillai.pdf)
by Pillai et al.

The tool itself is a little rough and ready and has not been maintained so
needed some changes to be useful which is why we use [a
fork](https://github.com/banks/alice) of the [original
code](https://github.com/madthanu/alice) in tree to make it easier to run these
tests. DGraph posted a blog about using ALICE on Badger (their KV store) a few
years ago which covered some of the same issues. Our fork also includes code
from [their fork](https://github.com/dgraph-io/alice) (though not all their
changes).

The changes to our fork are detailed [in the
README](https://github.com/banks/alice#changes-in-this-fork).

## Running tests

To run these test, you need Docker installed.

 1. `$ cd alice`
    Switch to the alice dir for all the below commands.
 2. `make test`
    This executes the `run-workload.sh` in the Docker container which runs the
    workload and the checker.

    It also creates and mounts `./traces_dir` to which strace output
    (checker input) and any failed test cases are copied for debugging.

    It takes about an hour to run all tests currently on my i9 (8 core) MacBook.

## Debug Failure

Understanding the failures is complex and we'll discuss in much more detail
below but we'll continue the cheat sheet on how to use this first. When there is
a test failure, the output will be copied to a directory like
`./logstore/traces_dir/reconstructeddir-*`. Work out which dir corresponds to
which failure with the guide below.

To debug it further the dir will contain the reconstructed WAL state, and three
other files.

```
$ ls ./traces_dir/reconstructeddir-14
wal-meta.db
// note no segment files because this case crashed before we created one
reconstructeddir-14.input_stdout
reconstructeddir-14.output_stdout
reconstructeddir-14.output_stderr
```

* `*.input_stdout` is the stdout from the workload up til the crash and is what
  you need to pass the the checker to re-run the test case under observation.
* `*.output_stdout` is stdout from the checker run
* `*.output_stderr` is stderr from the checker run. This is generally the first
  place to look to see what error caused the check failure.

You can re-run the checker yourself for this case with:

```
$ go run ./logstore/checker/main.go ./logstore/traces_dir/reconstructeddir-14 \
    ./logstore/traces_dir/reconstructeddir-14/reconstructeddir-14.input_stdout
```

In my case, I set up Delve in VSCode with a configuration like this (that I
edited for each case I explored).

```json
// launch.json
{
  "version": "0.2.0",
  "configurations": [
    {
      "name": "Run checker",
      "type": "go",
      "request": "launch",
      "mode": "debug",
      "program": "alice/checker/main.go",
      "args": [
        "../traces_dir/reconstructeddir-14/",
        "../traces_dir/reconstructeddir-14.input_stdout"
      ]
    }
  ]
}
```

I could then launch the checker in the debugger and set break points in WAL code
to step through and see where the failure was occurring during recovery etc.

I also wrote the workload in a way that makes it slightly easier to read a
hexdump of the .wal files directly with printable and recognizable log entry
payloads.

## Results

Interpreting the results is pretty tricky. Here are some example failures from
this library which we'll refer to when explaining below.


```
1. (Dynamic vulnerability) Across-syscall atomicity, sometimes concerning durability: Operations 13 until 16 need to be atomically persisted
2. (Dynamic vulnerability) Across-syscall atomicity, sometimes concerning durability: Operations 47 until 50 need to be atomically persisted
3. (Static vulnerability) Across-syscall atomicity: Operation /usr/local/Cellar/go/1.19/libexec/src/runtime/internal/syscall/asm_linux_amd64.s:36[runtime/internal/syscall.Syscall6] until /usr/local/Cellar/go/1.19/libexec/src/runtime/internal/syscall/asm_linux_amd64.s:36[runtime/internal/syscall.Syscall6]
4. (Dynamic vulnerability) Ordering: Operation 0 needs to be persisted before 15
5. (Static vulnerability) Ordering: Operation /usr/local/Cellar/go/1.19/libexec/src/runtime/internal/syscall/asm_linux_amd64.s:36[runtime/internal/syscall.Syscall6] needed before /usr/local/Cellar/go/1.19/libexec/src/runtime/internal/syscall/asm_linux_amd64.s:36[runtime/internal/syscall.Syscall6](Dynamic vulnerability)
(Dynamic vulnerability) Atomicity: Operation 1(garbage written semi-expanded (3 count splits), semi-expanded (4096 aligned splits), garbage written semi-expanded (4096 aligned splits), zero written, zero written semi-expanded (4096 aligned splits), garbage written, semi-expanded (3 count splits), zero written semi-expanded (3 count splits))
```

It helps to have a basic understanding of what ALICE is doing:

### ALICE Overview

ALICE first recorded the workload program (`logstore/workload/main.go` in our
case) running under it's modified version of strace which logs all the syscalls
it makes. One of the key modifications ALICE makes to strace is that the actual
data payload of each `write` call is written to a separate file called
`strace.out.byte_dump.*`. This allows ALICE to recreate the whole write
operation including the actual data written.

Then `alice-check` which is the python program in `./alice/bin` parses this
trace and builds a model of the syscalls that are relevant to disk writing.

When done, it outputs the set of _logical ops_ i.e. all operations that the
workload performed that altered state on disk (or wrote to stdout since that is
the synchronisation mechanism with the checker). These are numbered, and all the
tests will relate to running different combinations of these to simulate
different crashes.

Here's a snippet of logical ops output for the WAL:

```
10      write("wal-meta.db", offset=8192, count=4096, inode=4)
11      write("wal-meta.db", offset=12288, count=4096, inode=4)
12      fdatasync("wal-meta.db", size=32768, inode=4)
13      write("wal-meta.db", offset=4096, count=4096, inode=4)
14      fdatasync("wal-meta.db", size=32768, inode=4)
15      creat("00000000000000000001-0000000000000000.wal", parent=3, mode='0644', inode=5)
16      trunc("00000000000000000001-0000000000000000.wal", initial_size=0, inode=5, final_size=16384)
17      write("00000000000000000001-0000000000000000.wal", offset=0, count=16384, inode=5)
18      write("00000000000000000001-0000000000000000.wal", offset=0, count=8296, inode=5)
19      fsync("00000000000000000001-0000000000000000.wal", size=16384, inode=5)
20      fsync("/local_dir", size=80, inode=3)
21      stdout("'commitIdx=2\n'")
```
This set of ops shows BoltDB committing the metadata transaction that added the
initial WAL segment (10 - 14), then the creation of the new WAL file (15),
preallocation (16, 17), the initial write of header and log entries (18) and the
fsync to commit them (19) and then the fsync of the parent dir since this is the
first commit after create (20). Finally the stdout print is shown which is the
synchronisation with the checker.

For each of the logical ops, there might be multiple "micro ops". For example a
`write` of 10KiB might actually represent multiple smaller atomic write calls.
More on that in the testing details below.

Once ALICE has this set of ops. It then generates different crash simulations
based on its model of how filesystems might behave during crashes when
performing these operations. The way it generated those is detailed below.

To actually generate them though, it chooses a prefix of the logical operations
(and the micro ops within them) and converts those into a set of "disk ops"
which are essentially the set of writes/creates/truncates etc. needed to
recreate the same disk state up to that point. It then creates a new dir for
each scenario (in /dev/shm for speed) and then "replays" the set of disk
operations: for example by creating new files and writing data to them etc. For
writes it uses the data it stored in the `byte_dump` from strace to replay the
same bytes. It also takes all of the `stdout` writes the workload made up to the
point of the "crash" and stores them in a file.

Finally it executes the checker program (`./logstore/checker/main.go`) passing
it the reconstructed dir and the stdout up to the point of the simulated crash.
The checker then has to decide whether the state on disk is valid (i.e.
recoverable) and consistent with whatever the workload printed to stdout. In our
case the workload outputs the highest log entry it just committed so our checker
can validate that it can not only open the WAL OK but that every log up to that
index is still present and correctly recovered. There might be logs recovered
_after_ that index too depending on when the crash happens.

### Interpreting Test Output

Based on the example outputs given above. Here are some notes on how to
interpret them!

Each test scenario that fails the checker is called a "vulnerability" or "vuln"
in ALICE parlance.

#### `Dynamic` vs `Static

Every class of failure will have 1 or more `Dynamic` lines and a single `Static`
line. The difference is that the dynamic lines are the specific instances of
check failure found while static is an attempt to show which actual code lines
caused them.

In the example lines 1 and 2 correspond to atomicity issues found between
committing to BoltDB and the corresponding creation of a WAL segment file. This
happened twice in the run so two dynamic failures with different operation
numbers, but they ultimately are the same issue in code since both times the
same two lines of code were the call sites for the operations in question.

With Go, the static vulns are not all that useful as they all just point to the
syscall primitive within the stdlib rather than actual user code so for the most
part they can be ignored. But it's at least a bit useful to see that the
proceeding N dynamic failures trace to the same code paths.

#### Test Types

Without reading the code (mainly in `./alice/alicedefaultexplorer.py`) it's hard
to understand what these test are doing. Honestly even reading the code and
paper closely it's still a bit hard! But here's my current working
understanding.

There are roughly three phases of tests performed:

 1. **Across-syscall atomicity**: Vulns 1, 2 and 3 in the example above came
    from this. For each logical operation printed out, construct a directory
    after only operations 1-n are complete and then check if the checker can
    recover the state. Very roughly this phase is simulating a crash right after
    every logical disk operation to check if you rely on more than one
    non-atomic syscall to complete to leave the persistent state
    consistent/recoverable. For a failure of this type the output will be in
    `./logstore/traces_dir/reconstructeddir-{$n}` where `$n` is the last logical
    op replayed before the "crash". In example line 1 above there were
    reconstructed dirs for each operation from 13 to 16.

 2. **Ordering Vulnerabilities**: For each logical operation, "hide" (i.e. don't
    replay during reconstruction of the dir) all the disk operations involved in
    that logical operation, and then replay all subsequent ones. Each legal
    combination is tested (so this step could result in N^2 checks if they were
    all legal). Line 4 and 5 are an example of this kind of failure. For a
    failure of this type the output will be in
    `./logstore/traces_dir/reconstructeddir-{$i}-{$j}` where `$i` is the logical
    op omitted before the rest of the operations up to (and including) `$j` were
    replayed.

 3. **Atomicity Vulnerabilities**: For these tests, ALICE splits up the "micro
    ops" represented by each logical op in various ways and alters how it plays
    them back. For example a call that writes 16KiB of data to the log would be
    a single logical op in the output but may be split into multiple actual disk
    operations which won't necessarily be atomic.

    ALICE tests three different "modes" of splitting up the logical operations:
     * `(count, 1)`
     * `(count, 3)`
     * `(aligned, 4096)`

    In `(count, N)` modes, each operation is split into N so for example a 16KiB
    write in `(count, 1)` is written out in full as one "disk op", but in
    `(count, 3)` it's split into 3 separate `write` disk ops and a test case
    generated for each of those (i.e. it executes all disk ops for previous
    logical ops, and then only the first write of this op before "crashing").
    Logical operations other than write also have their own behavior, most
    interestingly, `truncate` has some interesting logic that also depends on
    whether the file is growing or shrinking. For example it might grow it only
    part of the way. `append` is a truncate but also may write garbage instead
    of the actual value for the last operation (which simulates the filesystem
    updating the size of the file in metadata but crashing before the actual
    write occurs). You might wonder what the point of `(count, 1)` mode is if
    it's not actually splitting writes/truncates etc. I think it's useful
    because some other operations like `rename` or `unlink` may be composed of
    multiple disk ops. For example `unlink` on a hardlink will first truncate
    the destination file before removing the dir entry. `(count, 1)` ensures
    that we test for recovery even if only part of that operation succeeds.

    In `(aligned, 4096)` mode, the operations are instead split on 4K page
    boundaries. This means large writes like 32KiB are split into up to 9
    separate writes (assuming it wasn't aligned to start with). This also
    applies to truncate and other ops too.

    OK, so now we know what those modes do, we can describe the actual test
    cases.

    * for each of the three split modes (which generate different sets of micro
      ops for each logical op as above):
      * for each prefix of the logical operations i in [0, NumOps]
        * for each prefix of the micro ops of the last logical op i
          * if this sequence of operations is valid for the fs model, generate a
            simulated crash at this point and check for recovery
        * also, for the `count` type splits only (to prevent state explosion),
          for each micro op j in i:
          * generate a crash with all of the other micro ops in i present but
            not micro op j, and run checker.
          * this simulated non-atomic writes/truncates etc.

#### Explain the output

Finally we can walk through the test output and explain what they mean.

```
2. (Dynamic vulnerability) Across-syscall atomicity, sometimes concerning durability: Operations 47 until 50 need to be atomically persisted
```

This is effectively the same issue as reported as 1 and 3 for the reasons given
above.

The logical operations in question at the time this failed looked like this:

```
47      write("wal-meta.db", offset=0, count=4096, inode=4)
48      fdatasync("wal-meta.db", size=32768, inode=4)
49      creat("00000000000000000017-0000000000000001.wal", parent=3, mode='0644', inode=6)
50      trunc("00000000000000000017-0000000000000001.wal", initial_size=0, inode=6, final_size=32768)
51      write("00000000000000000017-0000000000000001.wal", offset=0, count=32768, inode=6)
```

Apparently the "operations need to be atomically persisted" but this is a BoltDB
commit (47, 48) and WAL file create (49) and preallocate (50). How can those be
made atomic?

What that error actually means is that "if you crash _after_ operation 47, the
checker script errors, and the same for 48 and 49. Crashes after 50 and later
are OK again though.

So you have to work out _why_ the checker fails on each of these examples.
Actually there are _two separate bugs_ here in this one "failure"!

The first bug was that, although I accounted for it and called it out in the
design doc, if you crash _after_ adding a new segment to the Meta DB, but
_before_ actually creating the new file, recovery fails because RecoverTail
returns `ErrNoExist` and I wasn't handling that as designed.

The fix was to detect `ErrNoExist` on `RecoverTail` and handle it: if the
segment meta indicates that it was a branch new segment anyway with no actual
entries, then just recreate the empty file and move on. If it was not a new
segment then committed data has been lost and we can't recover! But that's not
the case in this error. We don need to be careful not to treat _any_ EOF as fine
though otherwise we'd silently loose committed logs if the tail file got
accidentally deleted!

You might wonder: why did this fail after 47 though when the BoltDB commit isn't
"done" til 48? The answer is that `fdatasync` is only the barrier. When
replaying the actual write will probably be reflected in filesystem even before
the fsync and so the checker will observe it even if it's not explicitly flushed
to disk yet. The same is true in real like - the write _might_ complete before
the fsync returns, it's just not guaranteed to!.

After that first fix, we no longer get a crash after 47 and 48, but we still do
after 49. This the second bug.

The second bug is that, although I explicitly handle the case where
pre-allocation failed or the file was still zero bytes in the recovery code... I
left behind a redundant read of the header from an earlier refactor that threw
EOF error attempting to read the expected file header! This wasn't even a case
of fixing it to not EOF because the correct code already did, I just needed to
delete that redundant read entirely!

```
4. (Dynamic vulnerability) Ordering: Operation 0 needs to be persisted before 15
```

This one is of the second type (a re-ordering check). What it means is that when
it omitted operation 0 (the create of meta-db) and all it's dependencies (i.e.
all other writes to that file) then tries to play all the other operations back
(i.e. creating the WAL files etc). Then the checker script failed.

At first I thought this was a false-positive. "But I _know_ that we don't write
WAL files until after the meta DB is created".

But it turns out this is a legit bug. Once I debugged the reconstruction it
became clear why. The reconstructed dir had no `wal-meta.db` but _did_ have the
first WAL segment file. On recovery we found no meta DB so initialized a new
one, and then, assuming an empty WAL state, tried to create the first WAL
segment but got `ErrExists` and failed!

I still thought: but this isn't a valid state, BoltDB is reliable and fsyncs!

But the real bug here is that although BoltDB fsyncs it's own DB file on
commits, it never fsync's it's parent direcroty after it's created itself for
the first time! That means that in a crash although the bolt file was safely
written to disk, the Directory Entry that points to it in the filesystem could
get lost resulting in exactly the scenario ALICE simulated here.

Just fsyncing the parent dir after initializing the MetaDB would be enough to
stop ALICE complaining here because ALICE's model of operation dependency
captures this fsync on the parent dir after a create and so it would not
consider this test case valid any more. But the actual fix was combined with the
final set of failures.

```
5. (Static vulnerability) Ordering: Operation /usr/local/Cellar/go/1.19/libexec/src/runtime/internal/syscall/asm_linux_amd64.s:36[runtime/internal/syscall.Syscall6] needed before /usr/local/Cellar/go/1.19/libexec/src/runtime/internal/syscall/asm_linux_amd64.s:36[runtime/internal/syscall.Syscall6](Dynamic vulnerability)
(Dynamic vulnerability) Atomicity: Operation 1(garbage written semi-expanded (3 count splits), semi-expanded (4096 aligned splits), garbage written semi-expanded (4096 aligned splits), zero written, zero written semi-expanded (4096 aligned splits), garbage written, semi-expanded (3 count splits), zero written semi-expanded (3 count splits))
```

This final failure is a summary of the many ways in which the type 3 Atomicity
Vulnerability tests failed. This one takes some interpreting but with the
understanding of what this type of test is doing above it makes some sense.

First of all, all of these issues are found with operation 1.

```
1       append("wal-meta.db", offset=0, count=16384, inode=4)
```

This is BoltDB creating a new database and writeing out the initial 16KiB file
with zeros. Because it's an append, it's both a truncate (i.e. changing the size
of the file) and a write (of zero in this case).

So in this test, ALICE starts mixing up the operations and only partially
truncating or writing garbage over the last split etc. The bit in parens
describes all the different ways it provoked a failure here (in separate test
that had separate reproduction dirs):
 * `garbage written semi-expanded (3 count splits)`: the 16KiB append wrote
   garbage to the last of the 3 splits.
 * `semi-expanded (4096 aligned splits)`: using 4K aligned splits but only
   appending a subset of them so it wasn't the full 16 KiB.
 * etc.

In all these cases what it's essentially telling us is that BoltDB isn't crash
safe when it's initializing! It is after this and in general. But when first
creating the DB file, if it's interrupted the result is a junk file that will
just throw an error when you try to open it which is why our checker fails.

At first I felt like this was BoltDBs fault and hopeless. It's also true that
our existing LogStore that uses BoltDB also has the same issue and it's never
come up in practice - crash during the very first few milliseconds of the very
first run is extremely unlikely and if it does happen and you get errors, it's
no big deal to manually delete the data dir and start again since there is no
data stored in it yet! I really didn't want to stat forking BBolt to fix this
because we have to know if it's initialization or opening an existing DB - one
case it's fine to delete and start over but one is _really_ not!

But, it's actually relatively easy for us to do better and solve all the BoltDB
init bugs in one go.

The fix is to not just rely on Bolt's implicit "create on open" behavior and
instead to control the init ourselves:
 1. Stat the file. If it already exists, just open it and continue.
 2. If not, delete any previous attempts to init stored in the tmp file name.
 3. Open a new BoltDB _under a tmp file name_, create buckets commit. etc.
 4. If that's all good, rename it to the real name.
 5. fsync the parent dir to ensure the new file is really there under the right
    name.
 6. Then close that and go back to 1.

After implementing that fix, all of the failures go away - the fsync on parent
dir prevents us ever "loosing" the meta DB after we start writing WAL segments,
and the atomic init process ensures that crash while BBolt is creating itself
are distinguishable from later DB errors because they are done on a tmp file we
can safely delete on next startup.