## master / unreleased
 - [FEATURE] New public `Scanner interface` for allowing to build custom CLI tools to repair or handle unrepairable blocks.
 - [FEATURE] New `scan` command for the `tsdb cli` tool that used the new tsdb scan interface to run a repair and move all unrepairable blocks out of the data folder to unblock Prometheus at the next startup.
 - [CHANGE] New `WALSegmentSize` option to override the `DefaultOptions.WALSegmentSize`. Added to allow using smaller wal files. For example using tmpfs on a RPI to minimise the SD card wear out from the constant WAL writes. As part of this change the `DefaultOptions.WALSegmentSize` constant was also exposed.

## 0.3.1
 - [BUGFIX] Fixed most windows test and some actual bugs for unclosed file readers.

## 0.3.0
 - [CHANGE] `LastCheckpoint()` used to return just the segment name and now it returns the full relative path.
 - [CHANGE] `NewSegmentsRangeReader()` can now read over miltiple wal ranges by using the new `SegmentRange{}` struct.
 - [CHANGE] `CorruptionErr{}` now also exposes the Segment `Dir` which is added when displaying any errors.
 - [CHANGE] `Head.Init()` is changed to `Head.Init(minValidTime int64)`  
 - [CHANGE] `SymbolTable()` renamed to `SymbolTableSize()` to make the name consistent with the  `Block{ symbolTableSize uint64 }` field.
 - [CHANGE] `wal.Reader{}` now exposes `Segment()` for the current segment being read  and `Offset()` for the current offset.
