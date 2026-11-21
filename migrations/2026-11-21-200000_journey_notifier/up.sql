-- Journey notifier stores device credentials, selected journeys and durable delivery state.
-- Device secrets are SHA-256 hashes. FCM device tokens are delivery addresses, not auth keys.
CREATE TABLE journey_notifier_devices (
    id TEXT PRIMARY KEY,
    secret_hash TEXT NOT NULL,
    fcm_token TEXT NOT NULL,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL
);
CREATE TABLE journey_notifier_journeys (
    device_id TEXT NOT NULL REFERENCES journey_notifier_devices(id) ON DELETE CASCADE,
    journey_id TEXT NOT NULL,
    legs JSONB NOT NULL,
    scheduled_end BIGINT NOT NULL,
    expires_at BIGINT NOT NULL,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    PRIMARY KEY (device_id, journey_id)
);
CREATE INDEX journey_notifier_journeys_expiry ON journey_notifier_journeys(expires_at);
CREATE TABLE journey_notifier_leg_states (
    device_id TEXT NOT NULL,
    journey_id TEXT NOT NULL,
    leg_index INTEGER NOT NULL,
    state JSONB NOT NULL,
    PRIMARY KEY (device_id, journey_id, leg_index),
    FOREIGN KEY (device_id, journey_id) REFERENCES journey_notifier_journeys(device_id, journey_id) ON DELETE CASCADE
);
CREATE TABLE journey_notifier_events (
    id TEXT PRIMARY KEY,
    device_id TEXT NOT NULL,
    journey_id TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    kind TEXT NOT NULL,
    title TEXT NOT NULL,
    body TEXT NOT NULL,
    sent_at BIGINT,
    next_attempt BIGINT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    created_at BIGINT NOT NULL,
    UNIQUE (device_id, journey_id, fingerprint),
    FOREIGN KEY (device_id, journey_id) REFERENCES journey_notifier_journeys(device_id, journey_id) ON DELETE CASCADE
);
CREATE INDEX journey_notifier_events_outbox ON journey_notifier_events(next_attempt, created_at) WHERE sent_at IS NULL;
