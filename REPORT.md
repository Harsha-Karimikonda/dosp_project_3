# Bonus Report — Node and Connection Failure Models in Pastry

## 1. Overview

In a wide-area peer-to-peer deployment, peers crash unpredictably and underlying network connections experience transient packet loss or permanent disconnection. To evaluate our Erlang Pastry overlay and key–value object service under faults, we implemented:

1. **Fault Injection Actor (`src/pastry_faults.erl`):**
   - **Node Crash Model (`--kill P`):** Selects `P%` of active peers uniformly at random and terminates their Erlang actors (`exit(Pid, kill)`) at random timestamps during the active request window.
   - **Connection / Link Failure Model (`--link-fail P --link-mode temp|perm`):** Selects `P%` of peers and severs bidirectional communication (`{fail_link, Peer, Duration}`) between each chosen peer and one of its active overlay neighbors in $L \cup R \cup M$.
     - In **`temp` (temporary)** mode, packets sent over the failed link are silently dropped for a random outage window of `1,000–3,000 ms`, after which the link automatically heals (`{heal_link, Peer}`).
     - In **`perm` (permanent)** mode, the bidirectional link remains dead for the lifetime of the simulation.

2. **Failure Detection, Detour Routing, and State Repair (`src/pastry_node.erl`):**
   - **Distinguishing Node Crashes from Pairwise Link Failures (`choose_hop/3`):** When a direct link `Self <-> NPid` is severed (`maps:is_key(NPid, dead_links)`), `NPid` itself has not crashed and remains the legitimate owner of its key range on the ring. Instead of evicting `NPid` from the leaf set (which would cause an adjacent node to falsely claim ownership of `NPid`'s keys), `choose_hop/3` keeps `NPid` in the ring state and **detours the routed packet (`Link detours`)** through the live, reachable peer in $L \cup R \cup M$ numerically closest to `Key` (excluding `dead_links` and already-visited nodes in `Path`), which then forwards the packet across its intact link to `NPid`.
   - **Per-Hop Acknowledgements & Crash Re-Routing:** Every forwarded `route` message carries a unique `AckRef` and arms a `300 ms` timer (`?ACK_TIMEOUT`). If the timer fires (`{ack_timeout, Ref}`) and the next hop is not merely a local dead link, the sender marks the crashed node as suspect for `?SUSPECT_TTL = 5,000 ms`, removes it from local state (`pastry_state:remove/2`), triggers state repair, and **re-routes the buffered packet** (`Re-routes (ack t/o)`) toward the new closest live owner.
   - **Explicit Path Cycle Detection:** Every routed packet carries the list of visited `NodeId`s (`Path`). Whenever a node receives a routed packet whose `Path` already contains its own `SelfId`, it increments the `loops` counter (`Routing loops`), distinguishing actual routing cycles from the `64`-hop safety cutoff (`Dropped (hop limit)`).
   - **Leaf-Set Keep-Alive Monitoring & Repair (`leaf_req` / `leaf_resp`):** Each peer monitors its leaf-set members (`erlang:monitor(process, Pid)`). Whenever a leaf crashes (and periodically every `2,000 ms` if `|Side| < |L|/2`), the node queries its farthest live leaf on that side for its leaf set and merges the fresh candidates, filtering out any node in its suspect table.
   - **Routing-Table & Neighborhood Repair (`rt_req` / `rt_resp`, `nbr_req` / `nbr_resp`):** Following Section 2.4 of Rowstron & Druschel, when slot `R[l][d]` fails due to a node crash, the node queries live entries in row `l` (and falls back to row `l + 1`) asking for their entry at `(l, d)`; when a neighborhood member crashes, it queries remaining `M` peers.
   - **Overlay Relay for Direct Sends (`xmit_direct`):** If an application calls `pastry_node:send/3` to a peer whose direct link is down, the node relays `{relay, TargetPid, Payload}` through a randomly chosen live neighbor whose link is up.

3. **Versioned Key–Value Object Replication (`src/kv_app.erl`):**
   - On every `kv_put(Key, Value)`, the primary owner assigns a monotonically increasing version `Ver = erlang:unique_integer([monotonic, positive])`, stores `{Ver, Value}` via `put_versioned/3` (which only overwrites if `Ver >= OldVer`), and replicates `{kv_replica, Key, {Ver, Value}}` onto the `K` numerically closest leaf-set neighbors (`K = 8` by default: 4 smaller + 4 larger).
   - Whenever the leaf set changes (`new_leafs/2` callback from Section 2.3), each node checks which stored keys it is now the primary owner for (`is_primary/2`) and pushes versioned replicas to its updated replica target set so the replication factor `K` is maintained across node crashes.

---

## 2. Experimental Methodology & Metric Definitions

We evaluated the system on a **`1,000`-node Pastry network** where each peer is configured to issue `R = 8` requests at `1 request/second` (`8,000` initial quota). Faults are injected uniformly between `t = 1.0 s` and `t = 4.8 s` while requests are actively in flight. Every run in the suite enables `--verify` and saves its full raw output to `results/<scenario>.txt`:

```bash
./tools/resilience.sh 1000 8
```

### Metric Definitions
- **Active Quota (Denominator for Delivery Success):** Initially `Expected = N * R` (`8,000`). When a peer crashes after having already reported $r_i$ completed requests to `pastry_stats`, only its *unreported remaining quota* ($R - r_i$) is subtracted from `Expected`. Thus, **Active Quota** equals all requests from surviving peers (`alive * R`) *plus* all requests that crashed peers completed and reported prior to crashing.
- **Delivery Success (%):** `Ok / Active Quota`, where `Ok` is the number of requests in the active quota that received a reply (`put` ack, `get` `{ok, Ver, Value}`, or `get` `not_found`) before the `5,000 ms` request timeout (`Timeouts = Active Quota - Ok`).
- **Misdelivered (`--verify`):** Checked against a global ground-truth `gb_sets` ring of all currently live node IDs maintained inside `pastry_stats`. A completed request is counted as `Misdelivered` if the `Owner` node that executed `deliver/4` is not the single live node globally closest to `Key` at the time the result is recorded.
- **Corrupt Values (`gets`, Provenance):** Every `kv_put(Key, Value)` registers `Value` in `pastry_stats` upon issue. Whenever a `kv_get(Key)` returns `{ok, Ver, V}`, `pastry_stats` checks that `V` was genuinely issued for `Key`.
- **Stale Reads (`gets`, Freshness):** Whenever a `kv_put(Key, Value)` is acknowledged with version `Ver` at timestamp `AckAt`, `pastry_stats` records `{AckAt, Ver}` for `Key`. When a `kv_get(Key)` issued at `GetIssuedAt` returns `{ok, ReadVer, V}`, `pastry_stats` compares `ReadVer` against $\max\{\text{Ver} \mid \text{AckAt} < \text{GetIssuedAt}\}$. If `ReadVer` is older than any write acknowledged before the `get` was issued, `Stale reads` is incremented.
- **Data Lost (`gets`):** Number of `kv_get(Key)` requests returning `not_found` for a `Key` whose `kv_put` had already been acknowledged at an earlier timestamp (`AckAt < GetIssuedAt`).
- **Routing Loops vs. Dropped (Hop Limit):** `Routing loops` counts how many times any routed packet visited a `NodeId` already present in its path history `Path`; `Dropped (hop limit)` counts packets exceeding `?MAX_HOPS = 64`.

---

## 3. Experimental Results

All raw logs for the table below are preserved in [`results/`](results/).

### 3.1 Verified Resilience Summary (`N = 1,000`, `R = 8`, `--verify` enabled on all runs)

| Scenario (Log File) | Killed / Alive | Links Failed | Active Quota (`Ok / Quota`) | Delivery Success | Misdelivered (`--verify`) | Corrupt Values | Stale Reads | Data Lost (`gets`) | Routing Loops | Re-routes | Link Detours | Repairs | Avg Hops |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| **Baseline** ([`baseline.txt`](results/baseline.txt)) | `0 / 1000` | `0` | `8000 / 8000` | **`100.00%`** | **`0`** | **`0`** | **`0`** | **`0`** | **`0`** | `0` | `0` | `0` | `2.4906` |
| **5% Node Crash** ([`kill_5.txt`](results/kill_5.txt)) | `50 / 950` | `0` | `7744 / 7744` | **`100.00%`** | **`0`** | **`0`** | **`0`** | **`0`** | **`0`** | `279` | `0` | `1,239` | `2.4919` |
| **10% Node Crash** ([`kill_10.txt`](results/kill_10.txt)) | `100 / 900` | `0` | `7496 / 7496` | **`100.00%`** | **`0`** | **`0`** | **`0`** | **`0`** | **`0`** | `641` | `0` | `2,560` | `2.4901` |
| **25% Node Crash** ([`kill_25.txt`](results/kill_25.txt)) | `250 / 750` | `0` | `6669 / 6675` | **`99.91%`** | **`0`** | **`0`** | **`0`** | **`0`** | **`0`** | `1,626` | `0` | `6,301` | `2.5122` |
| **50% Node Crash** ([`kill_50.txt`](results/kill_50.txt)) | `500 / 500` | `0` | `5327 / 5345` | **`99.66%`** | **`0`** | **`0`** | **`0`** | **`0`** | **`0`** | `3,051` | `0` | `12,153` | `2.4778` |
| **10% Link Temp** ([`link_temp_10.txt`](results/link_temp_10.txt)) | `0 / 1000` | `100` | `8000 / 8000` | **`100.00%`** | **`0`** | **`0`** | **`0`** | **`0`** | **`0`** | `0` | `15` | `0` | `2.4975` |
| **25% Link Temp** ([`link_temp_25.txt`](results/link_temp_25.txt)) | `0 / 1000` | `250` | `8000 / 8000` | **`100.00%`** | **`0`** | **`0`** | **`0`** | **`0`** | **`0`** | `0` | `38` | `0` | `2.5029` |
| **50% Link Temp** ([`link_temp_50.txt`](results/link_temp_50.txt)) | `0 / 1000` | `500` | `8000 / 8000` | **`100.00%`** | **`0`** | **`0`** | **`0`** | **`0`** | **`0`** | `0` | `79` | `0` | `2.4916` |
| **10% Link Perm** ([`link_perm_10.txt`](results/link_perm_10.txt)) | `0 / 1000` | `100` | `8000 / 8000` | **`100.00%`** | **`0`** | **`0`** | **`0`** | **`0`** | **`0`** | `0` | `49` | `0` | `2.5020` |
| **25% Link Perm** ([`link_perm_25.txt`](results/link_perm_25.txt)) | `0 / 1000` | `250` | `8000 / 8000` | **`100.00%`** | **`0`** | **`0`** | **`0`** | **`0`** | **`0`** | `0` | `115` | `0` | `2.4952` |
| **50% Link Perm** ([`link_perm_50.txt`](results/link_perm_50.txt)) | `0 / 1000` | `500` | `8000 / 8000` | **`100.00%`** | **`0`** | **`0`** | **`0`** | **`0`** | **`0`** | `0` | `236` | `0` | `2.5156` |
| **10% Kill + 25% Link Perm** ([`kill10_link25.txt`](results/kill10_link25.txt)) | `100 / 900` | `239` | `7482 / 7484` | **`99.97%`** | **`0`** | **`0`** | **`0`** | **`0`** | **`0`** | `693` | `124` | `2,568` | `2.5334` |
| **Ablation: 25% Kill, `--replicas 0`** ([`kill25_norepl.txt`](results/kill25_norepl.txt)) | `250 / 750` | `0` | `6695 / 6699` | **`99.94%`** | **`0`** | **`0`** | **`0`** | **`127`** | **`0`** | `1,647` | `0` | `6,386` | `2.4896` |

---

## 4. Findings & Analysis

1. **100% Exact-Owner Delivery (`Misdelivered = 0`) and Freshness (`Stale reads = 0`) Across All Scenarios:**
   - Across all 13 scenarios—including `50%` random node crashes, `50%` permanent link failures, and combined node + link failures—**`Misdelivered (verify) = 0`**, **`Corrupt values (gets) = 0`**, and **`Stale reads (gets) = 0`**. Every completed request arrived at the exact live node numerically closest to `Key` on the $2^{128}$ ring, and every `get` returned a value at least as recent as the latest `put` acknowledged prior to the `get`.

2. **Detour Routing Under Pairwise Connection Failures (`Link detours`):**
   - In all six link-failure experiments (`10%`, `25%`, `50%` × `temp` and `perm`), **`100.00%` (`8,000 / 8,000`) of requests succeeded** with **`0` misdeliveries** and **`0` lost reads**.
   - Keeping live peers in the leaf set while detouring around dead links via `choose_hop/3` (`15–79` detours under temporary link failures and `49–236` detours under permanent link failures) ensures that a broken edge `X <-> Z` routes `X -> Y -> Z` to the true owner `Z` rather than falsely treating `Z` as dead and delivering locally at `X`.

3. **Impact of Leaf-Set Replication (`--replicas 8` vs. `--replicas 0`):**
   - With `K = 8` versioned leaf-set replicas and re-replication on `newLeafs`, **`Data lost (gets) = 0` and `Stale reads (gets) = 0` across all four node-crash levels (`5%`, `10%`, `25%`, `50%`)**.
   - Disabling replication at `25%` node crash (`--kill 25 --replicas 0`) still routes `99.94%` of requests to the new closest surviving owner (`Misdelivered = 0`), but **`127` `get` requests return `not_found` for previously acknowledged keys (`Data lost = 127`)** because the original owner crashed with the sole copy.

4. **Empirical Hop Count and Path Cycle Detection:**
   - Across all fault scenarios, the empirical average hop count stayed between `2.4778` and `2.5334` (vs. `2.4906` in the fault-free baseline, max `5` hops).
   - Explicit path-cycle tracking (`lists:member(SelfId, Path)` at every hop) recorded **`Routing loops = 0` and `Dropped (hop limit) = 0`** across every run.
