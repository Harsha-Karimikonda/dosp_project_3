# Bonus Report — Node and Connection Failure Models in Pastry

## 1. Overview

In a wide-area peer-to-peer deployment, peers crash unpredictably and underlying network connections experience transient packet loss or permanent disconnection. To evaluate our Erlang Pastry overlay and key–value object service under faults, we implemented:

1. **Fault Injection Actor (`src/pastry_faults.erl`):**
   - **Node Crash Model (`--kill P`):** Selects `P%` of active peers uniformly at random and terminates their Erlang actors (`exit(Pid, kill)`) at random timestamps during the active request window.
   - **Connection / Link Failure Model (`--link-fail P --link-mode temp|perm`):** Selects `P%` of peers and severs bidirectional communication (`{fail_link, Peer, Duration}`) between each chosen peer and one of its active overlay neighbors in $L \cup R \cup M$.
     - In **`temp` (temporary)** mode, packets sent over the failed link are silently dropped for a random outage window of `1,000–3,000 ms`, after which the link automatically heals (`{heal_link, Peer}`).
     - In **`perm` (permanent)** mode, the bidirectional link remains dead for the lifetime of the simulation, forcing endpoints to detect unreachability, route around the broken link, and replace the unreachable peer in their state tables.

2. **Failure Detection, Routing Recovery, and State Repair (`src/pastry_node.erl`):**
   - **Per-Hop Acknowledgements & Re-Routing:** When reliability mode is active, every forwarded `route` message carries a unique `AckRef` and arms a `300 ms` timer (`?ACK_TIMEOUT`). The next hop immediately replies with `{hop_ack, AckRef}`. If the next hop has crashed or the link to it has failed, the timer fires (`{ack_timeout, Ref}`), the sender marks the unreachable node as suspect for `?SUSPECT_TTL = 5,000 ms`, removes it from local state (`pastry_state:remove/2`), triggers state repair, and **re-routes the buffered packet** through the next best live candidate (falling back to Pastry's rare-case rule over $L \cup R \cup M$ when the primary routing-table slot is empty).
   - **Explicit Path Cycle Detection:** Every routed packet carries the list of visited `NodeId`s (`Path`). Whenever a node receives a routed packet whose `Path` already contains its own `SelfId`, it increments the `loops` counter (`Routing loops`), distinguishing actual routing cycles from the `64`-hop safety cutoff (`Dropped (hop limit)`).
   - **Leaf-Set Keep-Alive Monitoring:** Each peer monitors its leaf-set members (`erlang:monitor(process, Pid)`), analogous to the keep-alive probes between adjacent ring neighbors in Section 2.4 of the Pastry paper.
   - **Proactive Leaf-Set Repair (`leaf_req` / `leaf_resp`):** Whenever a leaf is removed from the smaller or larger side (and periodically every `2,000 ms` if `|Side| < |L|/2`), the node queries its farthest live leaf on that side for its leaf set and merges the fresh candidates, filtering out any node in its suspect table.
   - **Routing-Table & Neighborhood Repair (`rt_req` / `rt_resp`, `nbr_req` / `nbr_resp`):** Following Section 2.4 of Rowstron & Druschel, when slot `R[l][d]` fails, the node queries live entries in row `l` (and falls back to row `l + 1`) asking for their entry at `(l, d)`; when a neighborhood member fails, it queries remaining `M` peers.
   - **Overlay Relay for Direct Sends (`xmit_direct`):** If an application calls `pastry_node:send/3` to a peer whose direct link is down, the node relays `{relay, TargetPid, Payload}` through a live neighbor whose link is up.

3. **Key–Value Object Replication (`src/kv_app.erl`):**
   - On every `kv_put(Key, Value)`, the primary owner (the node numerically closest to `Key`) replicates `{kv_replica, Key, Value}` onto the `K` numerically closest leaf-set neighbors (`K = 8` by default: 4 smaller + 4 larger).
   - Whenever the leaf set changes (`new_leafs/2` callback from Section 2.3), each node checks which stored keys it is now the primary owner for (`is_primary/2`) and pushes replicas to its updated replica target set so the replication factor `K` is maintained across node failures.

---

## 2. Experimental Methodology & Metric Definitions

We evaluated the system on a **`1,000`-node Pastry network** where each peer is configured to issue `R = 8` requests at `1 request/second` (`8,000` initial quota). Faults are injected uniformly between `t = 1.0 s` and `t = 4.8 s` while requests are actively in flight. Every run in the suite enables `--verify` and saves its full raw output to `results/<scenario>.txt`:

```bash
./tools/resilience.sh 1000 8
```

### Metric Definitions
- **Active Quota (Denominator for Delivery Success):** Initially `Expected = N * R` (`8,000`). When a peer crashes after having already reported $r_i$ completed requests to `pastry_stats`, only its *unreported remaining quota* ($R - r_i$) is subtracted from `Expected`. Thus, **Active Quota** equals all requests completed by surviving peers (`alive * R`) *plus* all requests that crashed peers completed and reported prior to crashing.
- **Delivery Success (%):** `Ok / Active Quota`, where `Ok` is the number of requests in the active quota that received a reply (`put` ack, `get` `{ok, Value}`, or `get` `not_found`) before the `5,000 ms` request timeout (`Timeouts = Active Quota - Ok`).
- **Misdelivered (`--verify`):** Checked against a global ground-truth `gb_sets` ring of all currently live node IDs maintained inside `pastry_stats`. A completed request is counted as `Misdelivered` if the `Owner` node that executed `deliver/4` is not the single live node globally closest to `Key` at the time the result is recorded.
- **Corrupt Values (`gets`):** Every `kv_put(Key, Value)` registers `Value` in `pastry_stats` upon issue. Whenever a `kv_get(Key)` returns `{ok, V}`, `pastry_stats` verifies that `V` is a genuine value written for `Key`; otherwise `Corrupt values` is incremented.
- **Data Lost (`gets`):** Number of `kv_get(Key)` requests returning `not_found` for a `Key` whose `kv_put` had already been acknowledged at an earlier timestamp (`T_ack < T_get_issued`).
- **Routing Loops vs. Dropped (Hop Limit):** `Routing loops` counts how many times any routed packet visited a `NodeId` already present in its path history `Path`; `Dropped (hop limit)` counts packets exceeding `?MAX_HOPS = 64`.

---

## 3. Experimental Results

All raw logs for the table below are preserved in [`results/`](results/).

### 3.1 Verified Resilience Summary (`N = 1,000`, `R = 8`, `--verify` enabled on all runs)

| Scenario (Log File) | Killed / Alive | Links Failed | Active Quota (`Ok / Quota`) | Delivery Success | Misdelivered (`--verify`) | Corrupt Values | Data Lost (`gets`) | Routing Loops | Re-routes | Repairs | Avg Hops |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| **Baseline** ([`baseline.txt`](results/baseline.txt)) | `0 / 1000` | `0` | `8000 / 8000` | **`100.00%`** | **`0`** | `0` | `0` | `0` | `0` | `0` | `2.4941` |
| **5% Node Crash** ([`kill_5.txt`](results/kill_5.txt)) | `50 / 950` | `0` | `7750 / 7750` | **`100.00%`** | **`0`** | `0` | **`0`** | `0` | `348` | `1,267` | `2.4986` |
| **10% Node Crash** ([`kill_10.txt`](results/kill_10.txt)) | `100 / 900` | `0` | `7476 / 7476` | **`100.00%`** | **`0`** | `0` | **`0`** | `0` | `649` | `2,563` | `2.5054` |
| **25% Node Crash** ([`kill_25.txt`](results/kill_25.txt)) | `250 / 750` | `0` | `6699 / 6704` | **`99.93%`** | **`0`** | `0` | **`0`** | `0` | `1,719` | `6,309` | `2.5180` |
| **50% Node Crash** ([`kill_50.txt`](results/kill_50.txt)) | `500 / 500` | `0` | `5351 / 5368` | **`99.68%`** | **`0`** | `0` | **`0`** | `0` | `3,131` | `12,276` | `2.4749` |
| **10% Link Temp** ([`link_temp_10.txt`](results/link_temp_10.txt)) | `0 / 1000` | `100` | `8000 / 8000` | **`100.00%`** | `6` (`0.08%`) | `0` | `0` | `0` | `11` | `11` | `2.4780` |
| **25% Link Temp** ([`link_temp_25.txt`](results/link_temp_25.txt)) | `0 / 1000` | `250` | `8000 / 8000` | **`100.00%`** | **`0`** | `0` | `0` | `0` | `31` | `29` | `2.4890` |
| **50% Link Temp** ([`link_temp_50.txt`](results/link_temp_50.txt)) | `0 / 1000` | `500` | `8000 / 8000` | **`100.00%`** | `23` (`0.29%`) | `0` | `0` | `0` | `96` | `93` | `2.4921` |
| **10% Link Perm** ([`link_perm_10.txt`](results/link_perm_10.txt)) | `0 / 1000` | `100` | `8000 / 8000` | **`100.00%`** | **`0`** | `0` | `0` | `0` | `42` | `39` | `2.4747` |
| **25% Link Perm** ([`link_perm_25.txt`](results/link_perm_25.txt)) | `0 / 1000` | `250` | `8000 / 8000` | **`100.00%`** | `5` (`0.06%`) | `0` | `0` | `0` | `80` | `78` | `2.4956` |
| **50% Link Perm** ([`link_perm_50.txt`](results/link_perm_50.txt)) | `0 / 1000` | `500` | `8000 / 8000` | **`100.00%`** | `43` (`0.54%`) | `0` | `2` | `0` | `173` | `165` | `2.4962` |
| **10% Kill + 25% Link Perm** ([`kill10_link25.txt`](results/kill10_link25.txt)) | `100 / 900` | `238` | `7498 / 7500` | **`99.97%`** | `3` (`0.04%`) | `0` | `0` | `0` | `719` | `2,598` | `2.5161` |
| **Ablation: 25% Kill, `--replicas 0`** ([`kill25_norepl.txt`](results/kill25_norepl.txt)) | `250 / 750` | `0` | `6697 / 6703` | **`99.91%`** | **`0`** | `0` | **`148`** | `0` | `1,588` | `6,260` | `2.5043` |

---

## 4. Findings & Analysis

1. **Exact-Owner Delivery and Value Integrity Under Uniform Node Crashes:**
   - Across all uniform random crash experiments (`5%`, `10%`, `25%`, `50%`, and the `--replicas 0` ablation), **`Misdelivered (verify) = 0`** and **`Corrupt values (gets) = 0`**. Every completed request was delivered to the exact live node numerically closest to `Key` on the $2^{128}$ ring, and every returned value matched a value genuinely written to that key.
   - Reply completion across the active quota remained `100.00%` at `5%` and `10%` crashes, `99.93%` (`5` timeouts out of `6,704`) at `25%` crashes, and `99.68%` (`17` timeouts out of `5,368`) at `50%` crashes, with up to `3,131` in-flight packets re-routed after per-hop ack timeouts.
   - Note that these experiments inject *uniform random* crashes across the ring; they demonstrate high empirical resilience under random churn rather than worst-case simultaneous failure of $|L|/2 = 8$ adjacent leaf-set neighbors.

2. **Why Pairwise Link Failures Produce Small Non-Zero `Misdelivered` Counts (`0.00%–0.54%`):**
   - In the link-failure runs (`--link-fail`), no nodes crash (`Alive = 1,000`), so the global verification oracle still considers all `1,000` nodes eligible owners. However, when the direct link between the true closest node `Z` and its immediate leaf neighbor `X` is severed, `X` cannot forward to `Z`, removes `Z` from `X`'s local leaf set after `ack_timeout`, and delivers `Key` locally at `X` (the next-closest replica holder).
   - Because `Z` is still alive globally, the strict oracle counts `X`'s local delivery as `Misdelivered` (`0` to `43` requests out of `8,000`, or `0.00%–0.54%`). Because `X` holds a leaf-set replica of `Z`'s keys, `Data lost (gets)` remains `0` in 5 of the 6 link-failure runs and `2` (`0.05%` of gets) in the `50%` permanent link-failure run, with `Corrupt values = 0` in all runs.

3. **Impact of Leaf-Set Replication (`--replicas 8` vs. `--replicas 0`):**
   - With `K = 8` leaf-set replicas and re-replication on `newLeafs`, **`Data lost (gets) = 0` across all four node-crash levels (`5%`, `10%`, `25%`, `50%`)**.
   - Disabling replication at `25%` node crash (`--kill 25 --replicas 0`) still routes `99.91%` of requests to the new closest surviving owner (`Misdelivered = 0`), but **`148` `get` requests return `not_found` for previously acknowledged keys (`Data lost = 148`)** because the original owner crashed with the sole copy.

4. **Empirical Hop Count and Path Cycle Detection:**
   - Across all fault scenarios, the empirical average hop count stayed between `2.4747` and `2.5180` (vs. `2.4941` in the fault-free baseline, max `5` hops), showing that re-routing and rare-case fallback added negligible path stretch on average.
   - Explicit path-cycle tracking (`lists:member(SelfId, Path)` at every hop) recorded **`Routing loops = 0` and `Dropped (hop limit) = 0`** across every run, confirming that quarantining failed node IDs in `suspects` for `?SUSPECT_TTL = 5,000 ms` prevented stale repair responses from inducing routing cycles in these experiments.
