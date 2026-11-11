**Persequor Dev Test**

Scope: 
+ No need to implement interfaces. Implement only the `TODO` comments as you see fit.
+ You can change anything you like. Frameworks, patterns, interfaces etc.
+ We suggest to work on it between 0.5 to 2 hours, but feel free to use as much time as you need.
+ Commit as often as you see fit.
+ There is no requirement to finish this, just to have some code for evaluation and further talks.
+ If you have time, review the code in the com.persequor.extensions package.

## Build and run

Use JDK 8 and Maven 3.9.11. The existing Mockito 1.x extension tests require JDK 8 for the commands below. Set `JAVA_HOME` and `PATH` to that JDK.

```sh
mvn clean verify
mvn -Dtest=StatisticsBenchmark test
```

The first command builds the JAR and runs the JUnit 4 suite, including deterministic listener, integration, and `EventService` tests. Expected rejection warnings and injected failure logs exercise the failure paths. The second command opts into an in-process benchmark. The normal build excludes it. `Ioc` still contains exercise placeholders. The tests wire the listeners to test adapters.

## Event storage

- Lookup queries the union of the incoming tracked item IDs. A single scan of overlapping history rejects the entire event if any relevant row is later. Compare `eventTime` first and `EventAction.getActionOrder()` second. Distinct UUIDs that compare equal are allowed. `recordTime` is optional ingestion metadata.
- Storage requires an event, UUID, event time, action, and a nonempty list of non-null IDs that remain nonempty after Java 8 `String.trim()`. IDs are compared as supplied and deduplicated for lookup. Statistics requires the event, event time, and the same valid ID list. Its count includes repeated list entries. Source and record time are optional for both listeners. The queue adapter supplies opaque delivery tags.
- A UUID already present in overlapping history is discarded and acknowledged even if the replay changes other fields. The repository can only reveal replays in the history it returns for the requested IDs. Global UUID uniqueness and replay detection across disjoint IDs or pruned history need a repository constraint or lookup. A null history result or malformed relevant row is a repository failure and leaves the delivery unacknowledged.
- Storage persists an accepted event, publishes to `statistics` and then `subscription`, and acknowledges last. Invalid, out-of-order, and detected replay deliveries get a warning and an acknowledgement with no storage or publication. Lookup, persistence, publication, and acknowledgement failures propagate with their causes. Later steps are skipped. A failed acknowledgement can have an uncertain broker outcome.
- Persistence followed by a failure can leave a stored event with neither or only one downstream publication. An acknowledgement failure can leave both publications in place. On redelivery, the UUID check will discard the replay if persistence is visible, so it cannot repair missing publications. A transactional outbox coupled with storage and idempotent downstream consumers would make retry safe.
- Run one storage listener instance as the sole writer for overlapping items. It serializes deliveries and the repository must make persisted rows visible to subsequent lookups. The concurrent-delivery test holds persistence open and verifies that a second delivery waits and then sees the committed event. Multiple writers would need one transaction or equivalent serialization covering **all** affected IDs for lookup and insert. A multi-item event requires one atomic decision.

## Statistics batching

- Flush at 10 deliveries or at the configured maximum age from the first pending delivery, whichever comes first. The default is one second. A delivery that sees an expired batch flushes it before opening a new batch. The deadline targets the start of a flush under healthy scheduling. Each flush sums list sizes by `eventTime.toLocalDate()`, writes once per day in order of first occurrence, and then acknowledges each tag in delivery order.
- The default constructor owns a single daemon scheduler. The configurable constructor borrows its scheduler. The caller keeps a borrowed scheduler running through listener shutdown and should use an executor that removes cancelled tasks promptly. Stop delivery and let active callbacks finish before calling `close()`, which flushes any healthy partial batch and shuts down an owned scheduler. Delivery, timeout, and close share a monitor, so a stale timeout cannot flush a later batch. At most ten tags are buffered in the listener. The queue adapter owns prefetch and backpressure.
- The port signatures are unchanged. Statistics calls the repository and queue from either the delivery, scheduler, or closing thread. A thread-affine queue adapter must marshal acknowledgements onto the tag's owning thread/channel, preserve individual acknowledgement semantics, and surface errors. Its dispatch must avoid a cycle where the channel thread waits to enter the listener while the listener waits for that thread. Adapters must avoid reentrant delivery from port calls and avoid mutating an event during delivery.
- A write failure stops the listener and leaves the whole batch unacknowledged. Earlier day updates may already have committed, and the failing write can have an uncertain outcome. An acknowledgement failure after successful writes stops the listener at that tag. Earlier acknowledgements may have succeeded. The listener logs failures, retains the failed batch, and surfaces the cause to a synchronous caller or on subsequent delivery or close. It never retries the batch itself. The adapter or supervisor should stop delivery and handle reconciliation or redelivery. Replaying unacknowledged tags can double-count. Reliable retries need durable idempotency keyed by event UUID, with deduplication and counter updates committed together. Atomic multi-day writes alone would still leave a write-to-ack gap.

## Benchmark and operational measurements

The benchmark runs the statistics listener with its real scheduler, an in-process acknowledgement recorder, and a counter-only repository. It warms up with three sparse deliveries and 50,000 sustained deliveries, then measures 10 deliveries spaced 1,100 ms apart and 200,000 deliveries submitted back-to-back. Every event contains two items. The date pattern is eight events on one day and two on the next.

Throughput uses the window from first arrival to last acknowledgement. The p95 is the nearest-rank percentile of per-delivery monotonic time from just before `handle` to the acknowledgement callback. Writes per batch are actual repository calls divided by acknowledged batches. The sparse sample is deliberately small, and the test adapters introduce no network or disk I/O.

One run on 2026-11-11 used Amazon Corretto 8u512, Maven 3.9.11, and Linux 6.18.57 x86-64 on an AMD EPYC 9V74 workspace with 9 processors visible to the JVM.

| Workload | Events | Batches | Events/s | p95 arrival-to-ack | Writes/batch |
| --- | ---: | ---: | ---: | ---: | ---: |
| Sparse | 10 | 10 | 0.92 | 1,000.763 ms | 1.00 |
| Sustained | 200,000 | 20,000 | 1,486,237.78 | 0.012 ms | 2.00 |

A proposed production measurement uses broker delivery to confirmed acknowledgement over a stated load, date mix, and adapter configuration. Record p95 across a defined window such as a 15-minute steady-state run. Track the failed-flush rate and the oldest pending delivery age alongside it. Treat the window as a proposal. The exercise defines no production latency threshold or SLA.
