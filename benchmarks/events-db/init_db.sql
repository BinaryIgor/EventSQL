CREATE DATABASE events;

CREATE USER events WITH password 'events';
ALTER DATABASE events OWNER TO events;

\c events;
SET ROLE events;

CREATE TABLE topic (
  name TEXT PRIMARY KEY,
  partitions SMALLINT NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE consumer (
  topic TEXT NOT NULL,
  name TEXT NOT NULL,
  partition SMALLINT NOT NULL,
  first_event_id BIGINT,
  last_event_id BIGINT,
  last_consumption_at TIMESTAMP,
  consumed_events BIGINT NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT NOW(),
  PRIMARY KEY (topic, name, partition)
);

CREATE TABLE event (
  topic TEXT NOT NULL,
  id BIGSERIAL NOT NULL,
  partition SMALLINT NOT NULL,
  key TEXT,
  value BYTEA NOT NULL,
  buffered_at TIMESTAMP NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT NOW(),
  metadata JSON NOT NULL,
  PRIMARY KEY (topic, id)
) PARTITION BY LIST (topic);

CREATE TABLE event_buffer (
  topic TEXT NOT NULL,
  id BIGSERIAL PRIMARY KEY,
  partition SMALLINT NOT NULL,
  key TEXT,
  value BYTEA NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT NOW(),
  metadata JSON NOT NULL
);

CREATE TABLE event_buffer_lock (
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);
INSERT INTO event_buffer_lock VALUES (DEFAULT);
