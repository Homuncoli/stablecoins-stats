-- =============================================================================
-- Migration: 0001_original -> 0002_tron (multi-chain schema)
-- =============================================================================

BEGIN;

-- -----------------------------------------------------------------------------
-- 1. Create the chain enum
-- -----------------------------------------------------------------------------
CREATE TYPE chain AS ENUM ('eth', 'tron', 'solana');


-- -----------------------------------------------------------------------------
-- 2. Create new multi-chain tables
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS blocks (
  chain          chain NOT NULL,
  number         BIGINT NOT NULL,
  ts             TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (chain, number)
);

CREATE TABLE IF NOT EXISTS addresses (
  chain   chain NOT NULL,
  addr    BYTEA NOT NULL,
  first   BIGINT NOT NULL,
  PRIMARY KEY (chain, addr),
  FOREIGN KEY (chain, first) REFERENCES blocks(chain, number)
);

CREATE TABLE IF NOT EXISTS tokens (
  chain   chain NOT NULL,
  id      SERIAL NOT NULL,
  addr    BYTEA NOT NULL,
  PRIMARY KEY (chain, id),
  UNIQUE (chain, addr)
);

CREATE TABLE IF NOT EXISTS transactions (
  chain                chain NOT NULL,
  block_number         BIGINT NOT NULL,
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

CREATE TABLE IF NOT EXISTS erc20_transfers (
  chain          chain NOT NULL,
  block_number   BIGINT NOT NULL,
  tx_index       INTEGER NOT NULL,
  log_index      INTEGER NOT NULL,
  token_id       INTEGER NOT NULL,
  from_id        BYTEA NOT NULL,
  to_id          BYTEA NOT NULL,
  amount         NUMERIC(78,0) NOT NULL,
  PRIMARY KEY (chain, block_number, tx_index, log_index),
  FOREIGN KEY (chain, block_number, tx_index) REFERENCES transactions(chain, block_number, tx_index),
  FOREIGN KEY (chain, from_id) REFERENCES addresses(chain, addr),
  FOREIGN KEY (chain, to_id) REFERENCES addresses(chain, addr)
);

CREATE TABLE IF NOT EXISTS token_events (
  chain          chain NOT NULL,
  block_number   BIGINT NOT NULL,
  tx_index       INTEGER NOT NULL,
  log_index      INTEGER NOT NULL,
  token_id       INTEGER NOT NULL,
  event_type     SMALLINT NOT NULL,
  a0_id          BYTEA,
  a1_id          BYTEA,
  value          NUMERIC(78,0),
  PRIMARY KEY (chain, block_number, tx_index, log_index),
  FOREIGN KEY (chain, block_number, tx_index) REFERENCES transactions(chain, block_number, tx_index),
  FOREIGN KEY (chain, a0_id) REFERENCES addresses(chain, addr),
  FOREIGN KEY (chain, a1_id) REFERENCES addresses(chain, addr)
);


-- -----------------------------------------------------------------------------
-- 3. Migrate data from old tables into new ones (all rows tagged as 'eth')
-- -----------------------------------------------------------------------------

-- blocks <- eth_block
INSERT INTO blocks (chain, number, ts)
SELECT 'eth', block_number, ts
FROM eth_block;

-- addresses <- address (addr integer id is dropped, addr bytea is the new key)
INSERT INTO addresses (chain, addr)
SELECT 'eth', addr
FROM address;

-- tokens <- token
INSERT INTO tokens (chain, addr)
SELECT 'eth', addr
FROM token;

-- transactions <- eth_tx
-- from_id/to_id were integer FKs to address(id), join to get the bytea addr
INSERT INTO transactions (
  chain, block_number, tx_index,
  from_id, to_id, method_id,
  value, gas_price, gas_used, effective_gas_price, success
)
SELECT
  'eth',
  t.block_number,
  t.tx_index,
  fa.addr,                    -- resolve integer from_id -> bytea addr
  ta.addr,                    -- resolve integer to_id   -> bytea addr (nullable)
  t.method_id,
  t.value,
  t.gas_price,
  t.gas_used,
  t.effective_gas_price,
  t.success
FROM eth_tx t
JOIN address fa ON fa.id = t.from_id
LEFT JOIN address ta ON ta.id = t.to_id;

-- erc20_transfers <- erc20_transfer
INSERT INTO erc20_transfers (
  chain, block_number, tx_index, log_index,
  token_id, from_id, to_id, amount
)
SELECT
  'eth',
  e.block_number,
  e.tx_index,
  e.log_index,
  e.token_id,
  fa.addr,
  ta.addr,
  e.amount
FROM erc20_transfer e
JOIN address fa ON fa.id = e.from_id
JOIN address ta ON ta.id = e.to_id;

-- token_events <- token_event
INSERT INTO token_events (
  chain, block_number, tx_index, log_index,
  token_id, event_type, a0_id, a1_id, value
)
SELECT
  'eth',
  te.block_number,
  te.tx_index,
  te.log_index,
  te.token_id,
  te.event_type,
  a0.addr,
  a1.addr,
  te.value
FROM token_event te
LEFT JOIN address a0 ON a0.id = te.a0_id
LEFT JOIN address a1 ON a1.id = te.a1_id;


-- -----------------------------------------------------------------------------
-- 4. Drop old tables (in FK-safe order) and old sequences
-- -----------------------------------------------------------------------------
DROP TABLE token_event;
DROP TABLE erc20_transfer;
DROP TABLE eth_tx;
DROP TABLE eth_block;
DROP TABLE token;
DROP TABLE address;

COMMIT;
