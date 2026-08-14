# Benchmark: native CRD versus offloaded to kcp

`test/bench` runs the same workload twice against the same cluster endpoint: once on an ordinary
CRD installed in the cluster, once on an identical resource served from kcp through spillway.

```console
make e2e-up     # kind + kcp + spillway
make bench
make e2e-down
```

The schemas are identical, the client is the same, and both are reached through the same
kube-apiserver. The only difference is where the objects live.

## What to expect before reading any numbers

**Spillway is slower, and it has to be.** A native custom resource is one hop — client,
kube-apiserver, etcd. An offloaded one is three — client, kube-apiserver acting as aggregator,
spillway, kcp, kcp's etcd. A benchmark showing spillway faster would be measuring something wrong.

Latency is the price. What is bought with it is the last table: what the cluster's own apiserver
and etcd stop carrying.

## Results

3000 objects, 16 workers, five runs, after a discarded 100-object warmup pass over every verb, on a
single-node kind cluster where kube-apiserver, etcd, kcp and spillway all compete for the same
cores. Treat the ratios as indicative and the absolute numbers as worthless off this machine.

Medians across the five runs; the range is the spread of each run's p50. 30,000 objects created in
total, no errors in any phase of any run.

The measured traffic runs as an ordinary ServiceAccount holding exactly one ClusterRole over both
widget resources — not as the cluster admin. That matters, and the section on authorization below
says why.

| phase | native ops/s | kcp ops/s | native p50 | kcp p50 | kcp p50 range | ratio |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| create | 311 | 169 | 47.8ms | 86.2ms | 83.4 – 89.9ms | 1.8× |
| get | 678 | 309 | 20.0ms | 49.3ms | 45.7 – 61.3ms | 2.2× |
| list (3000 objects) | 3.2 | 2.8 | 4.555s | 5.345s | 4.650 – 5.628s | 1.1× |
| patch | 320 | 151 | 43.4ms | 101.4ms | 93.9 – 105.6ms | 2.1× |
| delete | 268 | 137 | 54.4ms | 102.3ms | 100.5 – 109.0ms | 2.0× |
| watch notify | 116 | 66 | 7.9ms | 13.7ms | 12.6 – 14.8ms | 1.8× |

**Get was 2.7× until the authorization ceiling came off.** An earlier run of this same table
measured it at 247 operations a second against 676 native; it is now 309 against 678. Nothing about
the read path changed — what changed is that spillway no longer sends its `SubjectAccessReview`s
through a client capped at 200 a second, and get is the phase that was riding that cap. The section
below is where that came from.

The list row moved on both sides between those runs — 6.3 and 5.6 then, 3.4 and 3.0 now — with the
ratio unchanged. That is the machine, not the code, and it is the clearest reminder available that
the absolute numbers here mean nothing off this hardware.

Repeating the measurement is what makes those numbers usable. Before the warmup and the repeats,
the same code path produced p50s of 210ms and 109ms in consecutive runs; the spread across these
five is within about 10% for every phase except list.

**List is close to native at this size.** Throughput differs by about 12%, and the per-run ranges overlap
heavily — the slowest native run was slower than the fastest offloaded one. At 400 objects the same
comparison was 68% slower. The larger the response, the less the hops matter, because the
payload dominates. If your access pattern is list-and-watch, which is what controllers do, this is
the number that applies to you.

**Watch stays close**, at about 1.5×, and its p99 gap is under 10ms.

**Get is the one the earliest runs flattered.** Without a warmup at 400 objects it looked like
4.6×; warmed, repeated, at scale and with the authorization limiter out of the way it is 2.2×. It is
the phase with the least to amortise -- a single small object, three hops -- and the one that was
hitting a ceiling nobody had gone looking for.

**Patch was the outlier at 3.5–4×, and its ratio did not improve with scale** — which was the tell.
Everything else amortises the extra hops as the working set grows; patch did not, because its cost
was per request rather than per object. It is now 2.1×, in line with the other writes. See below.

## Where the time goes

Spillway's own metrics account for the kcp half of each request. These were gathered during an
earlier run of the table above rather than the current one, so read them as proportions rather than
against the milliseconds in that table:

| verb | mean time spillway waited on kcp |
| --- | ---: |
| get | 16.2ms |
| create | 44.4ms |
| delete | 55.6ms |
| patch | 62.5ms |
| list | 80.6ms |

For a get, 16ms of the 60ms the client saw was kcp; the rest is the aggregation layer, spillway,
and the client.

Patch is worse than that split suggests, because **admission adds round trips**. Counted directly
from spillway's metrics over 300 requests of each verb:

| client request | round trips to kcp |
| --- | --- |
| create | 1 — the create |
| delete | 2 — read the old object, then delete |
| patch | 3 — read the old object, ask kcp to resolve the patch with a dry run, then patch |

That is what admission needs in order to show a webhook the object as it will be stored.

Measured cost, three rounds of 300 patches each with and without patch admission, back to back on
the same cluster:

| | p50 | throughput |
| --- | ---: | ---: |
| admission enabled | 171 / 137 / 163ms | 90 / 110 / 88 ops/s |
| admission skipped | 105 / 77 / 88ms | 136 / 197 / 159 ops/s |

Taking medians, that was **about 75ms and roughly 40% of patch throughput**, and it is more
expensive than the patch itself: the two extra round trips average 28.3ms and 43.3ms of kcp time
against 60ms for the patch.

It was also being paid on a cluster with **no webhook configurations at all**. The admission chain
cannot say whether a webhook would act on a resource — its `Handles` reports whether the chain
handles the operation, and the webhook plugins answer yes unconditionally because they decide per
request. Spillway now consults the configurations directly, and only pays for the round trips when
one of them names the resource:

```
10 patches, no webhook configured
    {resource="widgets",verb="patch"}    -> 10

10 patches, a webhook matching widgets
    {resource="spillway",verb="read"}    -> 10
    {resource="spillway",verb="dryrun"}  -> 10
    {resource="widgets",verb="patch"}    -> 10
```

The check is biased towards doing the work: an unsynced cache, or the presence of any admission
policy binding, counts as a match. A wrong "no" would skip admission that should have run.

## What authorization costs

A native custom resource is authorized inside the kube-apiserver process. An offloaded one is
authorized twice: once there, before the request is proxied, and again by spillway — which, like
every aggregated API server, delegates the decision by POSTing a SubjectAccessReview back to the
cluster. That is a round trip per request, and it is the one part of the path an admin never
exercises: `system:masters` is in the always-allow list and is answered before the delegating
authorizer is reached.

The table above was therefore measured as an ordinary ServiceAccount. The cluster served **19,645
SubjectAccessReviews** during the run, against 34,155 offloaded requests — which is not a partial
cache hit rate but an exact accounting:

| requests | SubjectAccessReviews |
| --- | ---: |
| get, patch, delete — 19,300, each naming a distinct object | ~19,300 |
| create, list, watch — 15,155, no object name on the wire | ~0 |

The authorizer caches on the full attribute record, object name included. A create carries no name,
so 3000 of them share one cache entry; a get names one of 3000 objects, so it misses almost every
time. **The cost is per distinct object touched, not per request.**

What that cost is not, on this topology, is visible in the latency. Both columns below were measured
back to back, before the ceiling in the next section came off, so they compare identities honestly
with each other but not with the table at the top of this page:

| phase | ratio as admin | ratio as an RBAC user |
| --- | ---: | ---: |
| create | 1.8× | 1.9× |
| get | 2.8× | 2.7× |
| list | 1.0× | 1.1× |
| patch | 2.4× | 2.3× |
| delete | 1.9× | 2.2× |
| watch notify | 1.6× | 1.6× |

Every phase is within run-to-run spread of the admin numbers. On a single node the review is a
same-host hop, a couple of milliseconds against a 50–100ms request. Somewhere it would show is a
deployment where spillway does not sit next to the apiserver it delegates to, where each of those
19,300 reviews is a network round trip added to a request that already has three hops — and on the
apiserver itself, which served requests it would not otherwise have seen: about 19,600 of them in
each run of this benchmark, before and after.

### The ceiling underneath it

Those reviews are sent by a client that `k8s.io/apiserver` builds in a private helper, with
`QPS = 200`, `Burst = 400`, and no flag anywhere near them. Since the cache misses on every distinct
object, that is a ceiling on spillway's throughput — and it is reachable. 8000 objects, read once
each, 64 workers:

| offloaded phase | ops/s | one review per request? |
| --- | ---: | --- |
| create | 245.9 | no — a create names no object, so all 8000 share one cache entry |
| get | 210.4 | yes |
| patch | 189.2 | yes |
| delete | 190.0 | yes |

Create being *faster* than get is backwards for any storage system, and native get in the same run
reached 1489.8. The reviews themselves are the tell. A token bucket of rate `q` and burst `b` admits
at most `q + b/window` over any window, which for 200 and 400 is:

| window | the bucket permits | measured | with `--authorization-qps=800` |
| --- | ---: | ---: | ---: |
| 10s | 240.0/s | 234.9/s | 421.3/s |
| 20s | 220.0/s | 214.8/s | 369.0/s |
| 30s | 213.3/s | 209.4/s | 307.0/s |

Within 2.5% of the arithmetic bound at every window. That is a rate limiter, not a machine running
out of capacity. Raising it moved offloaded get from 210.4 to 376.5 ops/s and its p50 from 315.8ms
to 152.9ms.

Patch and delete did not move, and should not have: at roughly 190/s they were below the ceiling to
begin with, held there by what a write to kcp's etcd costs. The ceiling only binds a verb that could
otherwise go faster than 200/s. That is also why it does not appear in the main table above — at 16
workers only get approaches the line, and its phase is 400 requests, which the 400-deep burst
absorbs whole.

The default is now 800 rather than unlimited because every review is a request to the very apiserver
spillway is meant to be unloading. That trade is the operator's to make, which is what the flag is
for.

## What it does to everything else

Every number above says spillway is slower, which it is and must be. What the latency is supposed to
buy is that the cluster's own apiserver and etcd stop carrying the objects — so the question that
matters is what the rest of the cluster feels while three thousand of them are created.

Measured by writing and deleting one small ConfigMap every 200ms, which has nothing to do with the
workload, throughout each backend's create phase. A write rather than a read, because a read of an
unchanging object comes from the watch cache and would measure almost nothing; a ConfigMap goes to
etcd, which is the thing offloading is meant to protect.

| unrelated write, while | p50 | p95 | samples |
| --- | ---: | ---: | ---: |
| nothing else is happening | 17ms | 25ms | 49 |
| 3000 native objects are created | 52ms | 105ms | 243 |
| 3000 offloaded objects are created | 49ms | 135ms | 445 |

**Offloading did not protect the cluster here, and the tail got worse.** The medians are the same
within noise, and the 95th percentile is 30ms higher offloaded than native. On this topology that is
what should be expected and it is worth stating plainly rather than burying: kcp, spillway,
kube-apiserver and etcd are all on one machine competing for the same cores, so moving where the
objects are stored does not move the work off the box — and offloading adds spillway's own share of
it, including a `SubjectAccessReview` per distinct object.

The offloaded column is also under load for longer, which is why it has nearly twice the samples:
the same 3000 creates take 169 operations a second rather than 310.

So this measurement does not yet make the case for offloading. It makes the case for measuring it
somewhere the two control planes are not sharing a CPU, which is the only place the argument can be
tested — and it is now measured rather than assumed, which it was not before.

## What moved off the cluster

```
objects the cluster's own apiserver reports storing, at peak
  widgets.native.example.com    0 -> 3000
```

3000 native objects appear in the cluster's own storage metric. The 3000 offloaded widgets, created
through the same endpoint in the same run, **never appear at all** — no line for them, because that
apiserver is not storing them. They are not in its etcd, not in its watch cache, and not in its
resource count.

That is the trade the latency column pays for.

## Caveats

- Single-node kind: every component shares the same CPU, so spillway and kcp are competing with the
  kube-apiserver they are being compared against.
- 3000 objects still is not much. The interesting regime for offloading is where a cluster's etcd
  is under real pressure, which this does not reproduce.
- Five runs is better than one, but the spread above is still a range rather than a confidence
  interval, and every run shares one machine.
- kcp is storing to its own embedded etcd on an `emptyDir`.
- The authorization round trip is nearly free here because everything shares a host. Off-node, it is
  a network hop on every request that names an object.
- The unrelated-traffic measurement above is taken on one machine, where the two control planes share
  a CPU. It is the right measurement in the wrong place, and the place is the whole point.
