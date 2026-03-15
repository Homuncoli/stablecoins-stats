CREATE TYPE chain AS ENUM ('eth', 'tron', 'solana');

CREATE TABLE IF NOT EXISTS blocks (
  chain          chain NOT NULL,
  number         INTEGER NOT NULL,
  ts             TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (chain, number)
);

CREATE TABLE IF NOT EXISTS addresses (
  chain   chain NOT NULL,
  addr    BYTEA UNIQUE NOT NULL,
  PRIMARY KEY (chain, addr)
);

CREATE TABLE IF NOT EXISTS tokens (
  chain   chain NOT NULL,
  id      SERIAL NOT NULL,
  addr    BYTEA UNIQUE NOT NULL,
  PRIMARY KEY (chain, id)
);

CREATE TABLE IF NOT EXISTS transactions (
  chain                chain NOT NULL,

  block_number         INTEGER NOT NULL,
  tx_index             INTEGER NOT NULL,
  from_id              BYTEA NOT NULL,
  to_id                BYTEA,
  method_id            BYTEA,
  value                NUMERIC(78,0) NOT NULL DEFAULT 0,
  gas_price            NUMERIC(78,0),
  gas_used             BIGINT,
  effective_gas_price  NUMERIC(78,0),
  success              BOOLEAN,
  PRIMARY KEY (chain, block_number, tx_index),
  FOREIGN KEY (chain, block_number) REFERENCES blocks(chain, number),
  FOREIGN KEY (chain, from_id) REFERENCES addresses(chain, addr),
  FOREIGN KEY (chain, to_id) REFERENCES addresses(chain, addr)
);
