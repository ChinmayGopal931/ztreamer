# ztreamer

`ztreamer` is a heavily optimized zcash indexer that relies on [zakura](https://github.com/zakura-core/zakura) under the hood.

`ztreamer` additionally adds a `CompactTxStreamer` server over v2 p2p, which enables p2p light wallets.

## caveats
- currently, only a "direct" mode exists, comparable to zaino's "direct" mode

## `lightwallet-protocol` compatibility
All methods are supported, but two intentionally break the protocol spec, and one is still pending.

- `GetBlock` excludes transparent data, as no wallets try to access it and dont have a reason to.
- `GetBlockRange` rejects transparent filters. We believe transparent range scanning is not worth the bandwidth tradeoff, which we see in wallet adoption -- if wallets end up using requests this way one day, we will add it.
- `GetMempoolTx` is still unsupported.

## zaino parity
Other than the above caveats and gaps below, `ztreamer` has full request/response parity with zaino's "direct" mode.

24/27 JSON-RPC requests are served, with `getblockdeltas`, `getspentinfo`, and `gettxoutsetinfo` still pending.


## benchmarks

Still need to do another run with better memory profiling, but system memory usage peaked at 5GB while running other workloads.

```
   - Historical indexing: `116.61s`
   - Indexed height: `3,464,754`
   - Blocks written: `3,464,755`
   - Source bytes scanned: `273,277,465,462` bytes / ~`254.51 GiB`
   - Effective source throughput: ~`2.18 GiB/s` / ~`2,235 MiB/s`
   - Effective block rate: ~`29,712 blocks/s`
   - Total wall time including initial zakura archive node catchup: `183s`
   - Index size: ~`21 GiB`
   - Historical indexing CPU: ~`1.77` cores average / `5.5%` of 32 logical CPUs
   - System I/O wait during historical indexing: ~`32.7%`
 ```

I have yet to bench supplying clients, which will come next.
