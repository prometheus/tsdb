## master / unreleased

 - `LastCheckpoint` used to return just the segment name and now it returns the full relative path.
 - `NewSegmentsRangeReader` can now read over miltiple wal ranges by using the new `SegmentRange` struct.
 - `CorruptionErr` now also exposes the Segment `Dir` which is added when displaying any errors.
 - \[CHANGE\] Empty blocks are not written during compaction [#374](https://github.com/prometheus/tsdb/pull/374)