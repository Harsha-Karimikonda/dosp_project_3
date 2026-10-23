# Bonus Report — Node and Connection Failure Models in Pastry

## 1. Overview

In a real wide-area peer-to-peer deployment, nodes join and crash unpredictably and underlying IP links experience transient packet loss, congestion, or permanent routing blackholes. To evaluate and harden our Erlang Pastry overlay and key–value object service under realistic faults, we implemented:

1. **Fault Injection Actor (`src/pastry_faults.erl`):**
   - **Node Crash Model (`--kill P`):** Selects `P%` of active peers uniformly at random and terminates their Erlang actors (`exit(Pid, kill)`) at random timestamps during the active request window.
   - **Connection / Link Failure Model (`--link-fail P --link-mode temp|perm`):** Selects `P%` of peers and severs bidirectional communication (`{fail_link, Peer, Duration}`) between each chosen peer and one of its active overlay neighbors in $L \cup R \cup M$.
     - In **`temp` (temporary)** mode, packets sent over the failed link are silently dropped for a random outage window of `1,000–3,000 ms`, after which the link automatically heals (`{heal_link, Peer}`).
     - In **`perm` (permanent)** mode, the bidirectional link remains dead for the lifetime of the simulation, forcing nodes to detect the unreachability, route around the broken link, and replace the unreachable peer in their state tables.

2. **Failure Detection, Routing Recovery, and State Repair (`src/pastry_node.erl`):**
   - **Per-Hop Acknowledgements & Re-Routing:** When reliability mode is active, every forwarded `route` message carries a unique `AckRef` and arms a `300 ms` timer (`?ACK_TIMEOUT`). The next hop immediately replies with `{hop_ack, AckRef}`. If the next hop has crashed or the link to it has failed, the timer fires (`{ack_timeout, Ref}`), the sender marks the unreachable node as suspect for `?SUSPECT_TTL = 5,000 ms`, removes it from local state (`pastry_state:remove/2`), triggers state repair, and **immediately re-routes the buffered packet** through the next best live candidate (falling back to Pastry's rare-case rule over $L \cup R \cup M$ when the routing table slot is now empty).
   - **Leaf-Set Keep-Alive Monitoring:** Each peer monitors its leaf-set members (`erlang:monitor(process, Pid)`), matching Section 2.4 of the Pastry paper where adjacent ring neighbors exchange keep-alive probes. A crash produces an immediate `'DOWN'` message so the leaf set is repaired even before any data packet hits the dead node.
   - **Proactive Leaf-Set Repair (`leaf_req` / `leaf_resp`):** Whenever a leaf is removed from the smaller or larger side (and periodically every `2,000 ms` if `|Side| < |L|/2`), the node queries its farthest live leaf on that side for its leaf set and merges the fresh candidates, filtering out any node in its suspect table.
   - **Routing-Table Repair (`rt_req` / `rt_resp`):** Following Section 2.4 of Rowstron & Druschel, when slot `R[l][d]` fails, the node queries live entries in the same row `l` (and falls back to row `l + 1`) asking for their entry at `(l, d)`.
   - **Neighborhood-Set Repair (`nbr_req` / `nbr_resp`):** When a neighborhood member fails, the node queries its remaining `M` peers for their neighborhood sets and keeps the `|M| = 16` closest live nodes by Euclidean proximity.
   - **Overlay Relay for Direct Sends (`xmit_direct`):** If an application calls `pastry_node:send/3` to a peer whose direct link is down, the node relays `{relay, TargetPid, Payload}` through a live neighbor whose link is up.

3. **Key–Value Object Replication (`src/kv_app.erl`):**
   - On every `kv_put(Key, Value)`, the primary owner (the node numerically closest to `Key`) replicates `{kv_replica, Key, Value}` onto the `K` numerically closest leaf-set neighbors (`K = 8` by default: 4 smaller + 4 larger).
   - Whenever the leaf set changes (`new_leafs/2` callback from Section 2.3), each node checks which stored keys it is now the primary owner for (`is_primary/2`) and pushes replicas to its updated replica target set so the replication factor `K` is maintained across cascading node failures.

---

## 2. Experimental Methodology

We evaluated the system on a **`1,000`-node Pastry network** where every live peer issues `8` requests at `1 request/second` (`8,000` total requests in the fault-free baseline). Faults are injected uniformly between `t = 1.0 s` and `t = 4.8 s` while requests are actively in flight.

All experiments are reproducible via:
```bash
./tools/resilience.sh 1000 8
```

We measure:
- **Delivery Success (%):** Fraction of requests issued by live peers that receive a valid reply before timeout (`5,000 ms`).
- **Average Number of Hops:** Mean overlay hops taken by delivered requests.
- **Re-routes (`ack_timeout`):** Number of in-flight packets that hit a dead node or dead link, timed out after `300 ms`, and were successfully re-routed along an alternate path.
- **Repairs Triggered:** Number of times a node detected a failed peer in its $L$, $R$, or $M$ tables and initiated repair messages.
- **Data Lost (`gets`):** Number of `kv_get(Key)` operations that returned `not_found` for a `Key` whose `kv_put` had previously been acknowledged before the `kv_get` was issued.

---

## 3. Experimental Results

### 3.1 Summary Table (`N = 1,000`, `R = 8`)

| Scenario | Command Flags | Nodes Killed | Links Failed | Delivery Success | Avg Hops | Re-routes | Repairs Triggered | Data Lost (`gets`) |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| **Baseline (No Faults)** | `--verify` | `0` | `0` | **`100.00%`** | `2.4815` | `0` | `0` | `0` |
| **5% Node Crash** | `--kill 5` | `50` | `0` | **`99.99%`** | `2.5123` | `329` | `1,298` | **`0`** |
| **10% Node Crash** | `--kill 10` | `100` | `0` | **`100.00%`** | `2.4954` | `615` | `2,460` | **`0`** |
| **25% Node Crash** | `--kill 25` | `250` | `0` | **`99.91%`** | `2.5175` | `1,666` | `6,368` | **`0`** |
| **50% Node Crash** | `--kill 50` | `500` | `0` | **`99.76%`** | `2.4672` | `2,937` | `12,157` | **`0`** |
| **10% Link Fail (Temp)** | `--link-fail 10 --link-mode temp` | `0` | `100` | **`100.00%`** | `2.4872` | `15` | `14` | **`0`** |
| **25% Link Fail (Temp)** | `--link-fail 25 --link-mode temp` | `0` | `250` | **`100.00%`** | `2.4995` | `45` | `43` | **`0`** |
| **50% Link Fail (Temp)** | `--link-fail 50 --link-mode temp` | `0` | `500` | **`100.00%`** | `2.4931` | `76` | `74` | `1` |
| **10% Link Fail (Perm)** | `--link-fail 10 --link-mode perm` | `0` | `100` | **`100.00%`** | `2.4796` | `33` | `32` | `3` |
| **25% Link Fail (Perm)** | `--link-fail 25 --link-mode perm` | `0` | `250` | **`100.00%`** | `2.4955` | `93` | `89` | **`0`** |
| **50% Link Fail (Perm)** | `--link-fail 50 --link-mode perm` | `0` | `500` | **`100.00%`** | `2.4960` | `175` | `168` | **`0`** |
| **Combined 10% Kill + 25% Perm Link** | `--kill 10 --link-fail 25 --link-mode perm` | `100` | `240` | **`99.97%`** | `2.5084` | `754` | `2,627` | `2` |
| **Ablation: 25% Kill, No Replication** | `--kill 25 --replicas 0` | `250` | `0` | **`99.93%`** | `2.4965` | `1,520` | `6,141` | **`140`** |

---

## 4. Key Findings & Analysis

1. **Near-100% Routing Resilience Under Massive Churn:**
   - Even when **50% of all nodes (`500 / 1,000`) crash mid-experiment**, the overlay delivers **`99.76%`** of all requests from surviving nodes (`2,937` packets hit a dead next hop and are transparently re-routed).
   - This confirms Rowstron & Druschel's claim that Pastry routing is guaranteed to converge as long as fewer than $|L|/2 = 8$ adjacent nodes in the leaf set fail simultaneously.

2. **Hop Count Remains Bounded Around $\log_{16}(N)$:**
   - Across every failure scenario, the average hop count stays between **`2.467` and `2.518` hops** (compared to the fault-free baseline of `2.4815`).
   - When a primary routing-table entry `R[l][d]` is lost, Pastry's rare-case fallback (`shl(T, Key) >= l` and closer numerical distance) routes laterally by at most one extra hop while `rt_req`/`rt_resp` repairs the missing slot in the background.

3. **Temporary vs. Permanent Connection Failures:**
   - In all six link-failure experiments (`10%`, `25%`, `50%` × `temp` and `perm`), **100.00% of requests were delivered** with zero timeouts.
   - Permanent link failures trigger roughly **$2.1\times$ to $2.3\times$ as many re-routes and repairs** as temporary link failures (`175` vs. `76` at `50%`), because temporary links heal after `1–3 s` whereas permanent link failures permanently prune that edge from both endpoints' routing tables.
   - During development we observed that a failed direct link between a key's owner and the requesting peer could drop the direct `pastry_node:send/3` reply or leaf-set replica message. Adding `xmit_direct` (which relays direct sends through a live neighbor when the direct link is marked dead) eliminated direct-reply timeouts under link failures.

4. **Impact of Leaf-Set Replication (`--replicas 8` vs. `--replicas 0`):**
   - With `K = 8` leaf-set replicas and re-replication via the `newLeafs` callback, **zero previously acknowledged keys were lost (`Data lost = 0`) across 5%, 10%, 25%, and 50% node crashes**.
   - In the ablation run with replication disabled (`--kill 25 --replicas 0`), routing still succeeds (`99.93%` delivery to the new closest surviving node), but **`140` `get` requests lose their data** because the previous primary owner crashed with the only copy.
   - Under asymmetric link failures (`perm`/`temp`), a tiny fraction of reads (`0–3` out of `~4,000` gets, `< 0.08%`) can observe a split-view where one neighbor cannot reach the primary owner directly and delivers to an adjacent leaf while other neighbors still route to the live primary.

5. **Suspect Quarantine Window Prevents Routing Loops:**
   - When node `A` detects that node `B` is dead, `A` removes `B` and requests replacement state from neighbor `C`. If `C` has not yet noticed that `B` crashed, `C`'s reply would immediately re-insert `B` into `A`'s routing table, causing an infinite forward-timeout loop. Quarantining failed node IDs in a per-node `suspects` map for `?SUSPECT_TTL = 5,000 ms` completely prevented routing loops (`Dropped (hop limit) = 0` in every test).
