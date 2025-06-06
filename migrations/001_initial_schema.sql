-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Buckets table
CREATE TABLE buckets (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  name VARCHAR(255) UNIQUE NOT NULL,
  config JSONB NOT NULL,
  state INT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  deleted_at TIMESTAMPTZ
);

-- Create indexes for buckets
CREATE INDEX idx_buckets_name ON buckets(name);

CREATE INDEX idx_buckets_deleted_at ON buckets(deleted_at);

-- Streams table
CREATE TABLE streams (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  bucket_id UUID NOT NULL REFERENCES buckets(id) ON DELETE CASCADE,
  name VARCHAR(255) NOT NULL,
  config JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  deleted_at TIMESTAMPTZ,
  next_seq_id VARCHAR(255),
  last_timestamp TIMESTAMPTZ,
  UNIQUE (bucket_id, name)
);

-- Create indexes for streams
CREATE INDEX idx_streams_bucket_id_name ON streams(bucket_id, name);

CREATE INDEX idx_streams_deleted_at ON streams(deleted_at);

-- Access tokens table
CREATE TABLE access_tokens (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  token_value VARCHAR(255) UNIQUE NOT NULL,
  info JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  expires_at TIMESTAMPTZ
);

-- Create indexes for access tokens
CREATE INDEX idx_access_tokens_token_value ON access_tokens(token_value);

CREATE INDEX idx_access_tokens_expires_at ON access_tokens(expires_at);