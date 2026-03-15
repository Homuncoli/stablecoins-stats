CREATE TABLE IF NOT EXISTS eth_block (
  block_number   INTEGER PRIMARY KEY,
  ts             TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS address (
  id      SERIAL PRIMARY KEY,
  addr    BYTEA UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS token (
  id      SERIAL PRIMARY KEY,
  addr    BYTEA UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS eth_tx (
  block_number         INTEGER NOT NULL REFERENCES eth_block(block_number),
  tx_index             INTEGER NOT NULL,
  from_id              INTEGER NOT NULL REFERENCES address(id),
  to_id                INTEGER REFERENCES address(id),
  method_id            BYTEA,
  value                NUMERIC(78,0) NOT NULL DEFAULT 0,
  gas_price            NUMERIC(78,0),
  gas_used             BIGINT,
  effective_gas_price  NUMERIC(78,0),
  success              BOOLEAN,
  PRIMARY KEY (block_number, tx_index)
);

CREATE TABLE IF NOT EXISTS erc20_transfer (
  block_number   INTEGER NOT NULL,
  tx_index       INTEGER NOT NULL,
  log_index      INTEGER NOT NULL,
  token_id       INTEGER NOT NULL REFERENCES token(id),
  from_id        INTEGER NOT NULL REFERENCES address(id),
  to_id          INTEGER NOT NULL REFERENCES address(id),
  amount         NUMERIC(78,0) NOT NULL,
  PRIMARY KEY (block_number, tx_index, log_index),
  FOREIGN KEY (block_number, tx_index) REFERENCES eth_tx(block_number, tx_index)
);

CREATE TABLE IF NOT EXISTS token_event (
  block_number   INTEGER NOT NULL,
  tx_index       INTEGER NOT NULL,
  log_index      INTEGER NOT NULL,
  token_id       INTEGER NOT NULL REFERENCES token(id),
  event_type     SMALLINT NOT NULL,
  a0_id          INTEGER REFERENCES address(id),
  a1_id          INTEGER REFERENCES address(id),
  value          NUMERIC(78,0),
  PRIMARY KEY (block_number, tx_index, log_index),
  FOREIGN KEY (block_number, tx_index) REFERENCES eth_tx(block_number, tx_index)
);

ALTER TABLE eth_tx ADD COLUMN IF NOT EXISTS value NUMERIC(78,0) NOT NULL DEFAULT 0;
ALTER TABLE eth_tx ADD COLUMN IF NOT EXISTS gas_price NUMERIC(78,0);
ALTER TABLE eth_tx ADD COLUMN IF NOT EXISTS gas_used BIGINT;
ALTER TABLE eth_tx ADD COLUMN IF NOT EXISTS effective_gas_price NUMERIC(78,0);
ALTER TABLE eth_tx ADD COLUMN IF NOT EXISTS success BOOLEAN;