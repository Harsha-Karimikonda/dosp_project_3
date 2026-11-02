# Bonus Report — Node and Connection Failures

## Failure Models and Recovery

The implementation supports three failure models:

- **Node crash:** `--kill P` terminates `P%` of peer actors at random times during requests.
- **Temporary connection failure:** `--link-fail P --link-mode temp` drops messages in both directions between selected peers and an overlay neighbor for 1–3 seconds, then restores the connection.
- **Permanent connection failure:** `--link-fail P --link-mode perm` keeps those connections unavailable for the remainder of the run.

Peers detect node failures through process monitors and per-hop acknowledgement timeouts, reroute requests, and repair their overlay state. A broken connection can be bypassed through another peer. Stored objects are replicated to up to eight leaf-set neighbors by default in fault runs, and replicas are replenished when leaf sets change.

## How We Tested

Tests used Erlang/OTP 29.1.2 on macOS arm64, with 1,000 peers issuing eight requests each at one request per second. Faults were scheduled between 1.0 and 4.8 seconds after the request phase began. The results were generated on November 2, 2026 with:

```bash
make
./tools/resilience.sh 1000 8
```

The script tests node crashes, temporary and permanent connection failures, combined failures, and node crashes with replication disabled. It uses `--verify` to check delivered requests against the closest live destination. The baseline has no failures or replication; fault runs use eight replicas unless explicitly disabled. Fault runs use a five-second request timeout; the baseline uses 60 seconds.

**Active quota** includes all requests from surviving peers plus results already reported by peers before they crashed. Unreported requests from crashed origins are excluded. A reply can be a storage acknowledgement, a retrieved value, or `not_found`. Average hops counts the outward route for successfully replied requests, excluding replies, maintenance, and failed transmission attempts.

Connection-failure percentages select peers on which to schedule one failed link each; they are not percentages of all overlay edges. Crashed endpoints or repeated edges can reduce the number of distinct affected links.

## Results

Each row is one randomized run.

| Scenario | Replied / active quota | Timed out | Average hops |
| :--- | ---: | ---: | ---: |
| No failures | 8000 / 8000 | 0 | 2.4893 |
| 5% node crashes | 7749 / 7749 | 0 | 2.4863 |
| 10% node crashes | 7488 / 7490 | 2 | 2.4908 |
| 25% node crashes | 6707 / 6709 | 2 | 2.5007 |
| 50% node crashes | 5386 / 5412 | 26 | 2.4799 |
| 10% temporary link failures | 8000 / 8000 | 0 | 2.4787 |
| 25% temporary link failures | 8000 / 8000 | 0 | 2.4879 |
| 50% temporary link failures | 8000 / 8000 | 0 | 2.4951 |
| 10% permanent link failures | 8000 / 8000 | 0 | 2.4897 |
| 25% permanent link failures | 8000 / 8000 | 0 | 2.5096 |
| 50% permanent link failures | 8000 / 8000 | 0 | 2.5131 |
| 10% node crashes + 25% permanent link failures | 7468 / 7468 | 0 | 2.5289 |
| 25% node crashes, replication disabled | 6668 / 6673 | 5 | 2.5241 |

## Findings

- All temporary and permanent link-only tests completed every request. A separate controlled test confirmed successful reads during an outage and after automatic healing.
- Node-crash tests continued serving requests, with some timeouts at higher crash rates. At 50% crashes, 26 requests in the active quota timed out. The combined node/link test completed its active quota.
- The destination checks passed in every scenario. Replicated runs retained the previously stored objects checked by subsequent reads. Disabling replication caused reads of previously stored values to fail after owner crashes; a controlled replicated read succeeded after killing its owner.
- These tests demonstrate resilience to the implemented failures, with occasional request timeouts. Replication is asynchronous, so it does not guarantee preservation if the owner crashes before replicas receive a write or all copies are lost.

All failures were simulated between actors in one BEAM VM after network joins completed. These results describe the tested failure patterns, not arbitrary network partitions.
