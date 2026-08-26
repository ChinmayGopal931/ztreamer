# ztreamer

`ztreamer` is an optimized zcash indexer for supplying light wallets with `CompactBlock`s. 

Relies on zakura under the hood.

The point of this project is for me to make a CompactTxStreamer server over p2p, which is the last RPC to eliminate in light wallets. The optimizations I did were just nobrainers along the way.

## features
- Embeds zakurad and parses raw bytes from a db handle, to prevent the de- and re-compression of field elements that takes 91% of CPU in zaino
- fetching/building and LMDB write paths are pipelined now
- saves blocks in ranges of 1000, seals after 100, persists to LMDB after 10. Still handles deep reorgs just fine. Zaino only adds to db after 1000 confirmations, keeping these recent blocks in passthrough mode (on demand CompactBlocks, orders of magnitude slower).
- adds concurrency to initial indexing
- offers the supported CompactTxStreamer methods as a Zakura custom p2p service

## todos
- add daemon, logging, observability
- figure out how to do real benchmarks against zaino/zinder/lightwalletd
