# Persequor Dev Test

Reliability follow-up to merge `08af61c`. The PostgreSQL reference adapters demonstrate atomic event acceptance and publication intents, idempotent statistics updates, and ordering across concurrent storage writers. `EventService.handleRequest` and the `persist-queue`, `statistics`, and `subscription` queue names are unchanged.

## Build and verification

Use JDK 8, Maven 3.9.11, and PostgreSQL for the database checks. The existing Mockito extension tests use JDK 8. PostgreSQL 17.11 was used for verification.

```sh
mvn clean verify

# Point to a disposable database. The test role needs CREATE on that database.
export PSQR_JDBC_URL=jdbc:postgresql://localhost:5432/psqr_test
export PSQR_JDBC_USER=postgres
# Set PSQR_JDBC_PASSWORD if the server requires it.
mvn -Ppostgres-it clean verify
mvn -Dtest=ReliabilityBenchmark test
```

The default build runs JUnit unit tests and builds the JAR. The profile also runs the real PostgreSQL tests, each in a fresh schema created from the migration and dropped afterward. They use independent JDBC connections and observe actual PostgreSQL lock waits. The end-to-end test uses a deterministic in-process broker, enters through `EventService`, and checks committed rows from an independent connection at acknowledgement time. Expect warning and failure logs from deliberately injected faults. The benchmark is opt-in and asserts completion and consistency without latency thresholds.

For a new reference deployment, apply [V001__event_delivery.sql](src/main/resources/db/migration/V001__event_delivery.sql) once with your migration tooling, for example `psql -d psqr -v ON_ERROR_STOP=1 -f src/main/resources/db/migration/V001__event_delivery.sql`. Inject a PostgreSQL `DataSource` into the three `Postgres*Repository` adapters. Register the storage and statistics listeners on their queues and run `OutboxRelay.poll(limit)` repeatedly from a supervised worker, with a short wait when idle. `Ioc` still has exercise placeholders. Production broker wiring, subscription side effects, and process supervision are supplied by the deployment.

## Storage and publication

- The storage repository owns a `READ COMMITTED` transaction. It serializes each UUID with a transaction-scoped PostgreSQL advisory lock and locks all distinct tracked item rows in Java string sort order. Rows are created for previously unseen IDs, so overlapping first events also serialize. Every writer must follow this protocol. A UUID lock hash collision adds contention only. Disjoint writes have no shared application lock.
- The repository checks the latest committed event for each item while holding all item locks. It compares `eventTime`, retaining nanosecond precision, then `EventAction.getActionOrder()`. Distinct UUIDs that compare equal are valid. A later row for any item rejects the whole event. An accepted event, all updated item heads, and exactly two outbox rows commit together. A rejected transaction can leave empty item lock rows for previously unseen IDs.
- Replay is global by UUID. An identical replay matches event time, action, source, and the ordered tracked-ID list including duplicates. `recordTime` is ingestion metadata and may vary on replay. A UUID conflict or stale event is logged with the UUID and terminally acknowledged. An identical replay is acknowledged and existing intents remain eligible. A null event, UUID, event time, action, or missing or blank tracked IDs is terminally invalid. The PostgreSQL reference accepts valid UTF-8 item IDs of up to 1,024 bytes without NUL and a valid Unicode source when present. IDs are otherwise used as supplied.
- The storage listener acknowledges after the repository returns from commit. SQL failures propagate with their causes. There is no in-process transaction retry. The adapter must redeliver on transient database errors, including deadlock or lost commit responses, so the entire operation starts again. If the original commit succeeded, the durable UUID resolves the retry. Keep PostgreSQL durable commits enabled and apply bounded JDBC connection, statement, and lock timeouts appropriate to the deployment.
- The relay loads the immutable payload from the accepted row and uses `event UUID:destination` as the message ID. It waits at most the configured duration for broker confirmation. Failed or uncertain sends back off from 100 ms to a cap of 60 seconds. Pending intents remain durable and retry indefinitely. `failure_count`, `last_error`, and `next_attempt_at` expose stuck destinations. After repairing a route, an operator may move `next_attempt_at` to `clock_timestamp()` for the affected pending rows while the relay is stopped. Run the relay on a clock synchronized with PostgreSQL.
- Run **one active relay thread per database**. The supervisor must stop or fence the old process before starting a replacement. The reference has no claims or leases and does not support overlapping relay workers. After restart any unconfirmed intent becomes eligible when due. A send the broker accepted before confirmation was durably recorded can be repeated with the same message ID. The relay provides no FIFO guarantee across events or destinations. Subscribers should handle reordering and deduplicate by message ID or event UUID, committing their effect and deduplication key together.

## Broker and statistics contracts

- `EventQueue.push` accepts ingress durably before returning and surfaces rejection or unknown outcome. `ConfirmedPublisher.publish` submits promptly and completes successfully only on a durable, routed broker confirmation. Its adapter must correlate confirms and returns to individual sends, use durable routes and persistent messages, surface negative or unroutable outcomes, and clean up stalled confirmation state. A relay timeout bounds the wait. The broker may still accept that send.
- `EventQueue.acknowledge` applies to a single tag on its owning channel. Its adapter must surface detectable failures and permit redelivery of unacknowledged messages. For a thread-affine broker, marshal acks from the scheduler or closing thread without making the channel thread wait on a listener that is itself waiting on that channel. Use manual acknowledgements and bounded prefetch. Stop delivery and drain callbacks before closing the statistics listener. Port calls must avoid reentrant delivery into the same listener. Messages for statistics should come from the trusted relay path.
- Statistics flushes at 10 deliveries, at a configurable age from the first pending delivery (one second by default), or on healthy shutdown. The deadline is a target for starting the flush. The default listener owns its scheduler. With the injected scheduler, its caller keeps it running through close and should remove cancelled tasks promptly. Delivery, timeout, and shutdown share a monitor. The listener buffers at most ten tags.
- A statistics event without a UUID, event time, or valid nonempty tracked-ID list is logged and acknowledged without applying a count. The typed model represents malformed UUIDs as a decoding error, which the broker adapter must handle as poison input. Count every list entry, including repeated IDs, grouped by `eventTime.toLocalDate()`. The daily table uses the Java epoch-day number to preserve the full date range.
- The statistics repository owns a transaction. It acquires processing keys in UUID order using the unique constraint and only aggregates newly inserted keys. Within a batch the first copy of each UUID supplies the count. It increments daily rows in date order via atomic upserts. A failed transaction rolls back both keys and all daily changes. The listener acknowledges every tag individually after commit, including duplicates. On a write or acknowledgement failure it stops and surfaces the error, so a supervisor can restart and redeliver safely. Retain processing keys indefinitely in this exercise. Any finite policy must cover the longest possible broker, outbox, archive, and manual replay window.

## Failure matrix

| Boundary | Durable state | Acknowledgement and recovery |
| --- | --- | --- |
| Storage fails before commit | No accepted row or new intents | Leave ingress unacknowledged. Retry the full transaction. |
| Storage commit response or ack is lost | If committed, one event and two intents | Redelivery resolves by UUID. The relay can progress independently. |
| One destination rejects or times out | Other destination can be confirmed. The failed intent stays pending. | Ingress can already be acknowledged. Retry the pending destination after backoff. |
| Broker accepts before confirmation is recorded | Broker copy can exist while intent stays pending | Restart can publish a second copy with the same message ID. |
| A later statistics increment fails | No new keys or increments from that batch | Leave all its deliveries unacknowledged. Redeliver after recovery. |
| Statistics commit response or ack is lost | If committed, keys and totals exist together | Redeliver unacknowledged tags. Existing keys suppress a second increment. |

## Cutover from the previous exercise

Quiesce old writers and consumers, record a cutover watermark, and reconcile or drain outstanding deliveries. Import authoritative stored events with their original UUIDs and payloads. Seed each item head from the latest event by the existing comparator before enabling concurrent writers. Preserve known broker confirmations where they can be proven. For uncertain publications, reconcile against destination records and enqueue missing or uncertain intents only after downstream deduplication is in place. Historical subscriptions without reliable deduplication need reconciliation or explicit approval of duplicate side effects before replay.

Historical aggregate totals alone cannot establish which UUIDs were counted. Build a shadow statistics table and processing-key set together from a trusted per-event ledger, or rebuild both from the authoritative accepted events if all accepted history is meant to count. Validate the new totals and switch both tables at the same watermark. Seeding keys from guesses can miss counts. Keeping the old totals while replaying keyless history can double-count. If neither historical source is trustworthy, isolate legacy totals and quarantine old statistics deliveries until their effects can be reconciled.

## Local benchmark and service objective

The benchmark used Amazon Corretto 8u512, Maven 3.9.11, PostgreSQL 17.11, Linux 6.18.57 x86-64 on an AMD EPYC 9V74 workspace with nine JVM-visible processors, direct JDBC connections, and immediate simulated broker confirms. One relay drains statistics deliveries between polls. It warms up with 200 events at 100/s. The workload is 60% hot-item events, 40% two-item events, and an 80/20 date mix with a hot key per date. Every 100th event, starting with the first, gets one simulated subscription rejection.

Latency runs from return of the storage commit to the later of both simulated confirmations and the statistics commit. The benchmark samples the oldest unconfirmed intent about every 100 ms. Sparse submissions were spaced 1.1 seconds apart. Sustained submissions targeted 100 accepted events per second for about 10 seconds.

| Workload | Events | Window through completion | Accepted/s | Completed/s | p95 | p99 | Retries | Peak sampled oldest intent |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Sparse | 10 | 10.93 s | 1.01 | 0.91 | 1,029.47 ms | 1,029.47 ms | 1 | 104 ms |
| Sustained | 1,000 | 10.03 s | 100.06 | 99.71 | 110.30 ms | 128.14 ms | 10 | 113 ms |

Zero measured events took more than five seconds in this local run. The interview objective is **99.9% of accepted events completed within five seconds over a rolling 30-day window**. Start at the acceptance commit and end after both broker confirmations and the statistics effect commit. Use durable commit timestamps from PostgreSQL change capture and confirmation observations keyed by UUID and destination. Keep the earliest durable confirmation per destination. Where a confirmation observation is lost, wait for a later observable confirmation rather than inferring success.

Calculate the objective on unique accepted UUIDs, with cohorts evaluated once their five-second deadlines have passed. Keep late and incomplete events in the denominator as misses, including during retries or outages. Exclude rejections and duplicate deliveries. Alert on oldest unpublished intent by destination, retries and their rate, and oldest accepted event without a statistics processing key. Separate the latter by whether the statistics publish was confirmed to distinguish publication lag from consumer lag. Monitor missing telemetry and error-budget burn alongside these signals. The local run provides short-window evidence for simulated adapters. The 30-day objective requires production telemetry.
