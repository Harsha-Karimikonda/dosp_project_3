# Project 3 — Pastry Overlay Network & Object Access Service in Erlang

An Erlang actor-model implementation of the **Pastry** peer-to-peer routing substrate and a distributed key–value object access service, following:

> A. Rowstron and P. Druschel, *"Pastry: Scalable, decentralized object location and routing for large-scale peer-to-peer systems,"* Middleware 2001 (`http://rowstron.azurewebsites.net/PAST/pastry.pdf`).

---

## Team Members

| Name | UFID / GitHub |
| :--- | :--- |
| Harsha Karimikonda | `@Harsha-Karimikonda` |
| *(Add teammate if applicable)* | |

---

## Building and Running

### Prerequisites
- Erlang/OTP (tested on OTP 29; `erlc` and `escript` on `PATH`)

### Build
```bash
make
```
This compiles all Erlang modules in `src/` to `ebin/` with zero warnings and packs them into a self-contained executable escript `./project3` configured with `+P 4000000` so the BEAM VM can spawn hundreds of thousands of peer actors.

### Standard Usage
```bash
./project3 numNodes numRequests
```
Example:
```bash
./project3 1000 10
```
Each of the `numNodes` peers is an independent Erlang `gen_server` actor. Once all peers have joined the Pastry overlay, every peer issues `1` key–value request per second until it has completed `numRequests` requests. When all requests have finished, the program prints the summary statistics and the **average number of hops** traversed per request, then exits.

### Unit Tests
```bash
make test
```

### Bonus / Verification Flags
```bash
./project3 numNodes numRequests [options]
```
| Flag | Description |
| :--- | :--- |
| `--verify` | Verify against global ground truth that every delivered message arrived at the live node numerically closest to `Key` (`Misdelivered = 0`). |
| `--kill P` | **(Bonus)** Crash `P%` of peers at random times during the request phase (`exit(Pid, kill)`). |
| `--link-fail P` | **(Bonus)** Fail an overlay link on `P%` of peers during the request phase. |
| `--link-mode temp\|perm` | **(Bonus)** Temporary link failures heal after `1–3 s`; permanent link failures never heal. |
| `--replicas K` | **(Bonus)** Replicate each stored key onto the `K` numerically closest leaf-set neighbors (default `8` when fault injection is enabled). |
| `--keys K` | Size of the shared application key space (default `1000`). |
| `--timeout MS` | Per-request timeout in milliseconds. |

To run the full automated bonus resilience benchmark suite:
```bash
./tools/resilience.sh 1000 8
```

---

## Architecture & What Is Working

### 1. Pure Actor Model
- Every peer is a separate Erlang process (`gen_server` in `src/pastry_node.erl`).
- All routing, join protocol state transfer, leaf-set maintenance, failure detection, and application requests/replies are performed **exclusively via asynchronous Erlang message passing**. No shared memory (`ets`, `persistent_term`, etc.) or non-actor parallelism is used.

### 2. Pastry Protocol Parameters & State (`include/pastry.hrl`, `src/pastry_id.erl`, `src/pastry_state.erl`)
- **NodeId & Key space:** 128-bit unsigned integers (`0 .. 2^128 - 1`) generated via MD5 hashing (`erlang:md5/1`), viewed as 32 hexadecimal digits (`b = 4`, base `2^b = 16`).
- **Leaf Set (`L`):** `|L| = 16` (`8` numerically smaller + `8` numerically larger closest nodes on the `2^128` ring).
- **Routing Table (`R`):** up to `32` rows × `16` columns; slot `(l, d)` stores a node sharing an `l`-digit prefix with the local `nodeId` whose `(l+1)`-th digit is `d`, preferring closer nodes in the proximity space (Euclidean distance on 2-D coordinates).
- **Neighborhood Set (`M`):** `|M| = 16` closest nodes according to the proximity metric.

### 3. Network Join (`src/pastry_node.erl`, Section 2.4)
- A new node `X` contacts a nearby bootstrap node `A` (selected by proximity) and routes a `join_req` message toward `X`'s own `nodeId`.
- `A` sends `X` its neighborhood set `M`, every intermediate hop `i` sends `X` the routing-table rows `<= shl(X, i)`, and the final node `Z` (numerically closest to `X`) sends `X` its leaf set `L`.
- `X` initializes its state from the collected tables and announces its arrival (`{announce, X}`) to all members of its `L ∪ R ∪ M`, which update their own state accordingly.

### 4. Pastry Routing & Section 2.3 API (`src/pastry_node.erl`, `src/pastry_app.erl`)
- Implements the exact 3-branch routing algorithm from Section 2.2 / Table 1 of the paper:
  1. **Leaf-set range check:** if `Key` falls within `[min(L), max(L)]`, forward to the leaf-set member (or deliver to `self`) numerically closest to `Key`.
  2. **Prefix routing table lookup:** compute `l = shl(Key, Self)` and forward to `R[l][Key_l]` if present.
  3. **Rare case fallback:** forward to any node `T ∈ L ∪ R ∪ M` such that `shl(T, Key) >= l` and `dist(T, Key) < dist(Self, Key)`.
- Exposes the Section 2.3 Pastry API (`pastry_node:start/1` for `pastryInit`, `pastry_node:route/3`, `pastry_node:send/3`) and the application behaviour callbacks (`deliver/4`, `forward/4`, `new_leafs/2` in `src/pastry_app.erl`).

### 5. Key–Value Object Access Service (`src/kv_app.erl`)
- Implements `pastry_app` to associate 128-bit Pastry keys with strings (`kv_put` stores `(Key, String)` at the numerically closest node; `kv_get` looks up `Key` and replies to the requester).
- Each peer runs a 1 Hz timer (`erlang:send_after(1000, self(), kv_tick)`) and issues `numRequests` total requests.

### 6. Bonus: Node & Connection Failure Models (`src/pastry_faults.erl`, `src/pastry_node.erl`)
- **Node crash-stop failures (`--kill P`):** kills `P%` of peer actors during routing.
- **Temporary & permanent connection failures (`--link-fail P --link-mode temp|perm`):** drops packets along failed peer-to-peer links either for `1–3 s` or permanently.
- **Resilience mechanisms:** per-hop acknowledgements with `300 ms` timeout and automatic re-routing, leaf-set process monitoring, lazy routing-table repair (`rt_req`/`rt_resp`), proactive leaf-set & neighborhood repair (`leaf_req`/`leaf_resp`, `nbr_req`/`nbr_resp`), direct-send relay fallback around broken direct links, and `K`-way leaf-set object replication with automatic re-replication on `newLeafs`.
- Full methodology and experimental findings are documented in [`REPORT.md`](REPORT.md).

---

## Largest Network Managed & Scaling Results

All experiments below were run on an 8-core Apple Silicon Mac (16 GB RAM, Erlang/OTP 29).

| `numNodes` (`N`) | `numRequests` / node | Total Requests Delivered | Join Time (s) | Theoretical $\log_{16}(N)$ | **Measured Average Hops** | Max Hops | Misdelivered (`--verify`) | Peak Memory (RSS) |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `100` | `3` | `300` | `0.03 s` | `1.661` | **`1.7233`** | `3` | `0` | `48 MB` |
| `1,000` | `8` | `8,000` | `0.42 s` | `2.491` | **`2.4815`** | `4` | `0` | `140 MB` |
| `10,000` | `3` | `30,000` | `6.39 s` | `3.322` | **`3.2395`** | `5` | `0` | `1.08 GB` |
| `25,000` | `2` | `50,000` | `18.46 s` | `3.652` | **`3.5710`** | `5` | — | `1.45 GB` |
| `50,000` | `2` | `100,000` | `41.02 s` | `3.902` | **`3.7547`** | `6` | — | `2.36 GB` |
| **`100,000`** | `2` | **`200,000`** | `90.45 s` | `4.152` | **`3.9897`** | `6` | — | `4.34 GB` |

- **Largest network tested:** **`100,000` concurrent peer actors** routing **`200,000` requests** with **100% delivery (0 timeouts)** and an average of **`3.9897` hops** (matching the theoretical $\log_{16}(100{,}000) \approx 4.152$).
- Across all verified networks (`N = 2` up to `N = 10,000`), **`Misdelivered = 0`** — every single request landed at the exact live node numerically closest to the key on the $2^{128}$ ring.
