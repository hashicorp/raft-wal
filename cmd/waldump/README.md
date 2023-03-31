# waldump

A simple command for dumping the contents of WAL segment files to JSON for
debugging.

## Usage

```
$ waldump [-after INDEX] [-before INDEX] /path/to/wal/dir
...
{"Index":227281,"Term":4,"Type":0,"Data":"hpGEpUNvb3JkhKpBZGp1c3RtZW50yz7pEPrkTc4tpUVycm9yyz/B4NJg87MZpkhlaWdodMs/ABkEWHeDZqNWZWOYyz8FyF63P/XOyz8Fe2fyqYpayz7eXgvdsOWVyz7xX/ARy9MByz7XZq0fmx5eyz7x8ic7zxhJy78EgvusSgKUy77xVfw2sEr5pE5vZGWiczGpUGFydGl0aW9uoKdTZWdtZW50oA==","Extensions":null,"AppendedAt":"2023-03-23T12:24:05.440317Z"}
...
```

Each `raft.Log` is written out as JSON followed by a newline. The `Data` and
`Extensions` fields are opaque byte strings that will be base64 encoded.
Decoding those requires knowledge of the encoding used by the writing
application.

## Limitations

This tool is designed for debugging only. It does _not_ inspect the wal-meta
database. This has the nice property that you can safely dump the contexts of
WAL files even while the application is still writing to the WAL since we don't
have to take a lock on the meta database.

The downside is that this tool might in some edge cases output logs that have
already been deleted from the WAL. It's possible although extremely unlikely
that the WAL could be in the process of truncating the tail which could result
in there being both pre-truncate and post-truncate segment files present. This
tool might possibly output duplicate and out-of-order log indexes from before
and after the truncation. Or if `before` and `after` are used, it's possible we
might skip records entirely because an older file that has already been removed
was read instead of the newer one. These are all very unlikely in practice and
if the application that writes the WAL is still up and running are likely to be
resolved by the time you run the tool again.