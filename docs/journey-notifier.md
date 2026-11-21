# Journey notifier (Aspen -> PostgreSQL -> Firebase Cloud Messaging)

The `journey-notifier` Rust binary is deliberately separate from Edelweiss.
The app saves compact, pinned trip identities; a periodic worker asks Aspen only
for those trips and generates departure/platform/cancellation/transfer alerts.
FCM delivery does **not** require an Android background foreground service
or a persistent WebSocket.

## Firebase setup

1. Open the Firebase console and select/create the project used by the Android
   app (currently `catenary-android` in `catenary-compose/app/google-services.json`).
2. Enable **Cloud Messaging API (HTTP v1)** in the project's Google Cloud API
   settings. Ensure the messaging sender and app share the correct Firebase project.
3. Create a dedicated Google Cloud service account with the
   **Firebase Cloud Messaging API Admin** role for the target project. Download
   the JSON private key only to the server. Prefer workload identity when
   supported in production; this initial implementation uses a JSON file.
4. Put the JSON at, e.g., `/run/secrets/catenary-journey-fcm.json`,
   permission 0600, owned by the service user. **Never commit this JSON,
   its `private_key`, or an OAuth access token.** Set the server process
   `umask 077`. Only FCM registration *tokens* live in PostgreSQL.
5. Configure `catenaryconfig.toml` (not tracked), next to the other sections:

```toml
[postgres]
database_url = "postgres://.../catenary"

[aspen]
etcd_urls = ["127.0.0.1:2379"]

[journey_notifier]
listen = "127.0.0.1:40521"
poll_interval_seconds = 45
retention_hours = 6
max_monitor_days = 30

[journey_notifier.firebase]
project_id = "catenary-android"
service_account_path = "/run/secrets/catenary-journey-fcm.json"
```

6. Apply migrations to the same PostgreSQL instance as the backend:
   `diesel migration run` (or deploy your normal Diesel migrations). Then
   launch `cargo run --release --bin journey-notifier -- --config catenaryconfig.toml`.
7. Publish the local port **through HTTPS** using a reverse proxy. It must
   rate-limit `POST /v1/devices`, cap request sizes, and avoid access logs
   that print the `Authorization` header or request bodies. Do not expose
   the listener directly to the internet.
8. Build the Android app with `-PjourneyNotifierBaseUrl=https://YOUR_HOST`.
   See the paired `catenary-compose/docs/journey-notifications.md`.

### Network requirements

* Server -> Google OAuth token endpoint `https://oauth2.googleapis.com/token`.
* Server -> FCM HTTP v1
  `https://fcm.googleapis.com/v1/projects/PROJECT_ID/messages:send`.
* Server -> PostgreSQL and etcd. Etcd discovery resolves Aspen workers
  from `/aspen_assigned_chateaux/`.
* The Android app only needs HTTPS access to the notifier, and Google Play
  services/FCM to receive pushes. The **Firebase service account is never in
  the APK**.

## API and authentication

`POST /v1/devices` (first registration):

```json
{"fcmToken":"firebase-installation-registration-token"}
```

Returns a random `deviceId` and 256-bit `deviceSecret`. The secret is a
bearer credential, stored as SHA-256 on the server. The client retains it
outside Android backup and passes:

```
X-Device-Id: <deviceId>
Authorization: Bearer <deviceSecret>
```

`PUT /v1/devices/{deviceId}` updates a rotated FCM token.

`POST /v1/journeys/sync` fully reconciles one device's saved journeys
atomically, deleting server journeys absent from the request. It is safe to
retry a failed full synchronization. Removing a bookmark therefore removes
its notifier subscription on the next successful sync. An empty `journeys`
array removes all active subscriptions.

Example (timestamps are Unix seconds, all identifiers illustrative):

```json
{
  "journeys": [{
    "journeyId": "sha256-of-local-journey-identity",
    "scheduledEnd": 1795260000,
    "legs": [{
      "chateau": "feed-chateau",
      "tripId": "12345",
      "tripStartDate": "20261121",
      "tripStartTime": "08:24:00",
      "fromStopSequence": 3,
      "toStopSequence": 9,
      "fromStopId": "geneve",
      "toStopId": "lausanne",
      "fromName": "Genève",
      "toName": "Lausanne",
      "routeName": "IC",
      "tripShortName": "5",
      "timeZone": "Europe/Zurich",
      "scheduledDeparture": 1795245840,
      "scheduledArrival": 1795249440,
      "expectedDeparture": 1795245840,
      "expectedArrival": 1795249440,
      "expectedPlatform": "3",
      "transferSeconds": 120
    }]
  }]
}
```

The mobile app does **not** upload map geometry, street route shape, or
intermediate stop coordinates. It uploads only GTFS trip instances and the
necessary boarding, alighting, platform and transfer baselines.

## Monitoring semantics

Every `poll_interval_seconds`:

1. Query active subscriptions in PostgreSQL, group by `chateau` and
   `trip_id`, and discover their Aspen sockets using the etcd cache.
2. Fetch bounded selections via Aspen `get_all_trips_with_ids` RPC.
   Only consider realtime updates matching the exact GTFS **service day**
   and, when available, **start time**. Missing or ambiguous realtime
   identity is *unknown* and produces no notification.
3. Inspect the selected boarding and alighting `stop_sequence` (or
   scoped `stop_id` fallback). Read absolute event times, delays, the
   boarding platform, trip cancellation/deletion and skipped stops.
4. Compare with persisted per-leg state. On a meaningful change, insert a
   fingerprinted outbox event, guarded by a database UNIQUE constraint.
   Fingerprints are value-specific, so a new platform/time can alert again,
   but repeated identical Aspen polls cannot.
5. Compare predicted arrival of each transit leg with the next departure,
   including explicit walking transfer duration when provided. Only alert
   when the transfer transitions from feasible to infeasible.
6. Claim due outbox events with `FOR UPDATE SKIP LOCKED`, send through
   FCM HTTP v1, and record `sent_at`. Failed sends are retried.
   FCM is *at least once*, not exactly once; clients also deduplicate
   foreground deliveries by `eventId`.

Sample messages:

* "Change of platform in Genève. Departure at 08:24 now from platform 4."
* "IC 5 during your journey has been cancelled. Tap for more information."
* "Connection to RE 33 no longer possible due to delay. Please find an alternate connection."
* "IC 5 at Genève now departs at 08:27."

## Long delays and cleanup

* Initially, a journey is retained through at least 72 hours after its
  scheduled arrival, to avoid deleting monitors where Aspen lacks realtime.
* When the last transit leg is matched to realtime, the worker updates
  `expires_at` using the **predicted** arrival plus `retention_hours`
  (default six hours). Future delays extend expiry on subsequent polls.
* A configurable `max_monitor_days` (default 30) caps retention relative
  to subscription creation. This prevents unbounded retention when a
  service is indefinitely postponed or a feed malfunctions.
* After expiry, the process deletes journeys and their per-leg state and
  notifications in one cascade. A 30-day tombstone prevents a periodic
  mobile full-sync from recreating an already-expired journey.
* In unusually prolonged **feed outages**, an unknown delay beyond the
  conservative fallback cannot be inferred. Maintain monitoring health
  alerts and operational metrics for Aspen/FCM availability.

## Caveats and operational requirements

* This feature notifies timetable/realtime changes, **not passenger
  location**. No background GPS or boarding inference is involved.
* Exact transfer feasibility also depends on GTFS min-transfer rules,
  platform-specific walking times and the user's route details. The
  initial implementation uses the walking duration supplied by the
  client, or a conservative two-minute fallback. A subsequent iteration
  should revalidate infeasible transfers through Edelweiss.
* The notifier uses Aspen's **raw targeted trip-update RPC**, which
  includes platform strings. The routing-only source paging API excludes
  platforms, so it cannot satisfy platform notifications alone.
* FCM delivery is subject to permissions, device connectivity and OS
  behavior. The mobile app requests Android 13+ notification permission
  on save; notifications are never guaranteed to arrive at an exact time.
* HTTP routes currently use a device-scoped random bearer secret.
  Add edge rate limits, abuse detection and optional end-user account
  authentication before unrestricted public deployment.
* Keep PostgreSQL backups encrypted because FCM registration tokens are
  stored there. Do not expose token values in logs or analytics.
* Run one notifier worker initially; `SKIP LOCKED` protects send claims
  if multiple instances are deployed, but scanner processing itself
  should be leader-elected or serialized before horizontal scaling.

## Smoke test

1. Run migrations, start the notifier and check the health of etcd/Aspen.
2. Save a journey in Android. Verify a device and journey appear in
   `journey_notifier_devices` and `journey_notifier_journeys`.
3. Inject a realtime platform change for the selected trip/date/stop in
   Aspen. Verify exactly one outbox event and one FCM push.
4. Re-poll unchanged Aspen data: no new outbox row.
5. Delay an inbound connection past the outgoing train's departure plus
   required transfer time: verify a missed-connection event.
6. Cancel the selected trip: verify one cancellation event.
7. Remove the bookmark and let WorkManager reconcile: the remote
   subscription and its pending events are deleted.
8. Exercise token rotation, transient 5xx from Firebase and eventual
   retry, and delayed arrivals beyond the original scheduled end.
