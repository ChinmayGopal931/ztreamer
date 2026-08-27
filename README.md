# ztreamer

`ztreamer` is a heavily optimized zcash indexer for supplying light wallets with `CompactBlock`s.

Relies on zakura under the hood.

The point of this project is for me to make a `CompactTxStreamer` server over v2 p2p, which is the last RPC to eliminate in light wallets. The optimizations I did were just nobrainers along the way.

## features

- Embeds zakurad and parses raw bytes from a db handle, to prevent the de- and re-compression of field elements that takes 91% of CPU in zaino
- fetching/building and LMDB write paths are pipelined now
- saves blocks in ranges of 1000, seals after 100, persists to LMDB after 10. Still handles deep reorgs just fine. Zaino only adds to db after 1000 confirmations, keeping these recent blocks in passthrough mode (on demand CompactBlocks, orders of magnitude slower).
- adds concurrency to initial indexing
- offers the supported CompactTxStreamer methods as a Zakura custom p2p service

## benchmarks

The only thing getting saturated is my disk, implying even at these speeds, **much** heavier indexing workloads could be done.

```
- Historical indexing: 173.67s
- Indexed height: 3,461,594
- Blocks written: 3,461,595
- Source bytes scanned: 273,207,013,864 bytes / ~`254.45 GiB`
- Effective source throughput: ~`1.46 GiB/s / ~1500 MiB/s`
- Effective block rate: ~`19,932 blocks/s`
- Index size: ~`21G (data.mdb` is ~20.8 GiB)
- Max RSS: ~`6.0 GiB`
```

I have yet to bench supplying clients, which will come next.
