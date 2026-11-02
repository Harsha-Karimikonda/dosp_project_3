# Project 3 — Pastry in Erlang

An Erlang actor-model implementation of Pastry and a key-to-string object access service, based on [Rowstron and Druschel's Pastry paper](https://rowstron.azurewebsites.net/PAST/pastry.pdf).

## Team Members

| Name | Email | UFID |
| :--- | :--- | :--- |
| Harsha Karimikonda | hkarimkonda@ufl.edu | 50966091 |
| Venkatesh Gollepalli | ve.gollepalli@ufl.edu | 24690032 |

## Build and Run

Requires Erlang/OTP with `erlc` and `escript` on `PATH` (tested with OTP 29).

```bash
make
./project3 numNodes numRequests
```

For example:

```bash
./project3 1000 10
```

Each peer sends one request per second until it has issued `numRequests` requests. After the requests finish or time out, the program prints the average number of hops and exits. A local delivery counts as zero hops; the average covers successfully replied requests and excludes replies and maintenance messages.

To run the unit tests:

```bash
make test
```

## What Is Working

- One Erlang `gen_server` actor per peer, communicating through message passing.
- 128-bit Pastry identifiers, leaf sets, routing tables, and neighborhood sets.
- Network joins through routed join messages, state transfer, and arrival announcements.
- Leaf-set routing, prefix-table routing, and fallback routing.
- A similar API to Section 2.3: initialization, routing, direct sending, and application callbacks for delivery, forwarding, and leaf-set changes.
- A key-to-string service supporting storage and retrieval.
- One request per second per peer, request completion tracking, termination, and average-hop output.
- Bonus node crashes and temporary/permanent connection failures, with rerouting, state repair, and object replication. Testing and findings are in [REPORT.md](REPORT.md).

## Largest Network Managed & Scaling Results

The largest tested network used **200,000 peer actors**, each issuing **2 requests**. All **400,000 requests** were delivered, with an average of **4.2738 hops**. This is a previously recorded run.

| Number of nodes | Requests per node | Requests delivered | Average hops |
| ---: | ---: | ---: | ---: |
| 100 | 3 | 300 | 1.6767 |
| 1,000 | 8 | 8,000 | 2.5004 |
| 10,000 | 3 | 30,000 | 3.2410 |
| 25,000 | 2 | 50,000 | 3.5614 |
| 50,000 | 2 | 100,000 | 3.7554 |
| 100,000 | 2 | 200,000 | 3.9897 |
| 200,000 | 2 | 400,000 | 4.2738 |

These scaling experiments can be reproduced with `./tools/scale.sh`.

## Bonus Tests

```bash
./tools/resilience.sh 1000 8
```

Individual failure models can be run with:

```bash
./project3 1000 8 --kill 10
./project3 1000 8 --link-fail 25 --link-mode temp
./project3 1000 8 --link-fail 25 --link-mode perm
```

`--kill P` crashes `P%` of peers. `--link-fail P` schedules a connection failure on `P%` of peers. Temporary failures heal after 1–3 seconds; permanent failures remain for the run.
