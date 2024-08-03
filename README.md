Query engine and server for UK Open Rail Data TD ("Train Describer") feed
=========================================================================

Allows storage, indexing, and querying of archival data from the TD feed
as documented at https://wiki.openraildata.com/index.php?title=TD

The archival data is in 2 parts: "recent" and "archive". Recent data are
queried for time ranges including today and yesterday UTC, and archive
data for anything older than that.

A separate server (not included) should be:

- accepting the TD feed from publicdatafeeds.networkrail.co.uk
- serving the data on ~/var/collect_td_feed.sock
- writing the data to spool files in ~/var/tdfeed/batches

The spool files should be logs that just record the TdFrames in the order
they were received, each with a 4-byte LE length prefix, and named after
the UTC datestamp of the data.

A utility `mktdarchive` consumes the spool files, generates indices, and
writes them to `~/var/tdfeed`. The data files are in the same format as
the spool files except sorted by time and consolidated in case there were
multiple spool files for the same day. The index files are xz-compressed
TdIndex protos. `mktdarchive` only consumes spool files at least 12 hours
after the end of the day they contain, to allow for late-arriving messages.
It can be run as a cron job.

In `td-archive-server`, the `recent` module consumes the live feed from
`~/var/collect_td_feed.sock` into a memory ring buffer for a 2-day period
(the period that `archive` cannot cover) while the `archive` module reads
from the data and index files produced by `mktdarchive`.

The result is served on `~/var/tdfeed/sock` with each quer being directed
to one backend or the other or both as appropriate.

Bugs
====

The recent data are not backfilled, so there is a coverage gap until the
server has been up for 48 hours.

Everything is quite inconsistent and there are no tests.
