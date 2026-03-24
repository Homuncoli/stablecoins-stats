-- =============================================================================
-- Unified Normalized Blockchain Schema
-- Covers: Ethereum · Polygon (EVM-identical) · Tron (TRC10 + TRC20)
-- =============================================================================
--
-- Design principles:
--   1. Single set of core tables for all chains — no per-chain table forks
--   2. EVM chains (Ethereum, Polygon) and Tron share block/tx/receipt/log
--      structure. Differences captured in typed columns where universal,
--      meta JSONB where chain-specific.
--   3. TRC10 is a first-class table — not stuffed into transaction.meta
--   4. All addresses stored as lowercase 0x-prefixed hex (20 bytes)
--      Tron base58 encoding is application-layer only
--   5. All timestamps as bigint microseconds UTC
--      EVM seconds × 1_000_000 / Tron milliseconds × 1_000
--   6. Costs unified: execution_cost (gas on EVM, energy on Tron)
--      bandwidth is Tron-only — lives in receipt.meta
--   7. Reorg-safe: is_canonical on block + transaction, removed on log
--   8. chain_id on every table — multi-chain from day one
--
-- Table hierarchy:
--   chain
--   └── block
--       └── transaction
--           ├── receipt
--           ├── log
--           │   └── log_topic
--           └── internal_transaction
--   trc10_token          (Tron protocol-level token registry)
--   trc10_transfer       (Tron TRC10 transfer events — no logs emitted)
--   contract             (enrichment — populated via eth_call / Tron API)
--   function_selector    (4-byte selector registry for tx classification)
--   indexer_checkpoint   (operational — tracks indexed ranges)
--   reorg_event          (operational — audit log of reorgs)
--
-- Decoding layer (materialized views) lives at the bottom of this file.
-- =============================================================================


-- ---------------------------------------------------------------------------
-- Extensions
-- ---------------------------------------------------------------------------
CREATE EXTENSION IF NOT EXISTS pg_trgm;        -- trigram indexes for address search
CREATE EXTENSION IF NOT EXISTS btree_gist;     -- for range exclusion constraints


-- =============================================================================
-- CHAIN
-- Reference table. Every other table FK's to this.
-- vm_type drives which meta fields are populated and which APIs to call.
-- =============================================================================
CREATE TABLE chain (
    chain_id            integer         PRIMARY KEY,
    name                text            NOT NULL,
    network             text            NOT NULL,
    vm_type             text            NOT NULL
                            CHECK (vm_type IN ('evm', 'tron')),
    -- cost_unit: label for what execution_cost_used measures
    -- 'gas' on EVM chains, 'energy' on Tron
    cost_unit           text            NOT NULL DEFAULT 'gas',
    -- native_decimals: decimal places of the chain's native token
    -- ETH/MATIC = 18 (wei), TRX = 6 (sun)
    native_decimals     smallint        NOT NULL DEFAULT 18,
    native_symbol       text            NOT NULL DEFAULT 'ETH',
    is_testnet          boolean         NOT NULL DEFAULT false,
    notes               text
);

INSERT INTO chain
    (chain_id, name,            network,    vm_type, cost_unit, native_decimals, native_symbol)
VALUES
    (1,        'mainnet',       'ethereum', 'evm',   'gas',     18,              'ETH'),
    (137,      'polygon-pos',   'polygon',  'evm',   'gas',     18,              'MATIC'),
    (1000,     'tron-mainnet',  'tron',     'tron',  'energy',  6,               'TRX'),
    -- testnets
    (11155111, 'sepolia',       'ethereum', 'evm',   'gas',     18,              'ETH'),
    (80002,    'polygon-amoy',  'polygon',  'evm',   'gas',     18,              'MATIC'),
    (2494104990,'tron-shasta',  'tron',     'tron',  'energy',  6,               'TRX');


-- =============================================================================
-- BLOCK
--
-- Column mapping:
--   EVM miner / Tron witness_address  →  block_producer
--   EVM difficulty                    →  difficulty  (null post-merge, null on Tron)
--   EVM nonce                         →  nonce       (null post-merge, null on Tron)
--   EVM base_fee_per_gas              →  base_fee_per_gas (null on Tron, null pre-EIP1559)
--   Tron witness_signature            →  meta->>'witness_signature'
--   Tron version                      →  meta->>'version'
--   Tron bandwidth_limit              →  meta->>'bandwidth_limit'
-- =============================================================================
CREATE TABLE block (
    chain_id            integer         NOT NULL
                            REFERENCES chain (chain_id),
    number              bigint          NOT NULL,
    hash                text            NOT NULL,
    parent_hash         text            NOT NULL,

    -- Normalised to microseconds UTC across all chains
    -- EVM:  block.timestamp (seconds) × 1_000_000
    -- Tron: block.block_header.raw_data.timestamp (ms) × 1_000
    timestamp_us        bigint          NOT NULL,

    block_producer      text,           -- EVM: miner/fee_recipient  Tron: witness_address
    gas_limit           bigint,         -- EVM only
    gas_used            bigint,         -- EVM only
    base_fee_per_gas    bigint,         -- EVM EIP-1559 only (null pre-London, null on Tron)
    size_bytes          bigint,
    transaction_count   integer         NOT NULL DEFAULT 0,

    -- Trie roots (EVM). Tron stores equivalent in block header but field names differ.
    transactions_root   text,
    receipts_root       text,
    state_root          text,

    -- Bloom filter over all logs in block — EVM only, enables fast eth_getLogs
    logs_bloom          text,           -- 0x-prefixed 256 bytes; null on Tron

    -- Pre-merge EVM fields
    difficulty          numeric,        -- null post-merge and on Tron
    total_difficulty    numeric,        -- null post-merge and on Tron
    nonce               text,           -- null post-merge and on Tron

    is_canonical        boolean         NOT NULL DEFAULT true,

    -- Chain-specific overflow
    -- Tron shape:  { "version": 27, "witness_signature": "0x...", "bandwidth_limit": 43200000 }
    -- EVM shape:   { "extra_data": "0x...", "mixHash": "0x...", "sha3Uncles": "0x..." }
    meta                jsonb,

    PRIMARY KEY (chain_id, hash)
);

-- Only one canonical block per height
CREATE UNIQUE INDEX block_canonical_number_idx
    ON block (chain_id, number)
    WHERE is_canonical = true;

CREATE INDEX block_timestamp_idx
    ON block (chain_id, timestamp_us);

CREATE INDEX block_producer_idx
    ON block (chain_id, block_producer)
    WHERE block_producer IS NOT NULL;


-- =============================================================================
-- TRANSACTION
--
-- Column mapping:
--   Both chains:
--     from_address, to_address, value, input  → direct columns
--
--   EVM:
--     nonce                     → nonce
--     gas (limit)               → cost_limit
--     gasPrice                  → cost_per_unit        (null for EIP-1559)
--     maxFeePerGas              → max_cost_per_unit    (EIP-1559)
--     maxPriorityFeePerGas      → max_priority_cost_per_unit (EIP-1559)
--     type (0/1/2/3)            → transaction_type
--     accessList                → meta->>'access_list'
--
--   Tron:
--     fee_limit (energy cap)    → cost_limit
--     expiration timestamp      → meta->>'expiration'
--     contract_type string      → mapped to transaction_type integer:
--                                   0 = TransferContract (TRX)
--                                   1 = TriggerSmartContract (contract call)
--                                   2 = CreateSmartContract (deployment)
--                                   3 = TransferAssetContract (TRC10) — see trc10_transfer
--                                   9 = other (freeze, vote, etc.)
--     raw contract_type string  → meta->>'contract_type'
--     bandwidth_usage           → meta->>'bandwidth_usage'
--     extra_calls (multi-call)  → meta->>'extra_calls'
--
-- NOTE: TRC10 transfers (transaction_type = 3) are duplicated into the
-- trc10_transfer table for easier querying. The raw transaction row is
-- still stored here for completeness and replayability.
-- =============================================================================
CREATE TABLE transaction (
    chain_id                        integer     NOT NULL
                                        REFERENCES chain (chain_id),
    hash                            text        NOT NULL,
    block_hash                      text        NOT NULL,
    block_number                    bigint      NOT NULL,
    transaction_index               integer     NOT NULL,

    from_address                    text        NOT NULL,
    to_address                      text,               -- null on contract creation

    -- Value in native smallest unit (wei on EVM, sun on Tron)
    value                           numeric     NOT NULL DEFAULT 0,

    -- Calldata (EVM) or serialised contract call payload (Tron TriggerSmartContract)
    input                           text,               -- 0x-prefixed

    -- EVM-only sequential replay protection. Null on Tron (uses expiration instead).
    nonce                           bigint,

    -- Unified cost model
    -- EVM:  gas limit set by sender
    -- Tron: fee_limit (max energy the sender will pay for)
    cost_limit                      bigint,

    -- EVM legacy/EIP-2930 gas price. Null for EIP-1559 txns and all Tron txns.
    cost_per_unit                   bigint,

    -- EVM EIP-1559 fee fields. Null for legacy txns and all Tron txns.
    max_cost_per_unit               bigint,
    max_priority_cost_per_unit      bigint,

    -- EVM: 0=legacy, 1=EIP-2930, 2=EIP-1559, 3=EIP-4844 (blob)
    -- Tron: see mapping above
    transaction_type                smallint    NOT NULL DEFAULT 0,

    is_canonical                    boolean     NOT NULL DEFAULT true,

    -- Chain-specific overflow (see mapping above)
    meta                            jsonb,

    PRIMARY KEY (chain_id, hash),

    FOREIGN KEY (chain_id, block_hash)
        REFERENCES block (chain_id, hash)
);

CREATE INDEX transaction_block_hash_idx
    ON transaction (chain_id, block_hash);

CREATE INDEX transaction_block_number_idx
    ON transaction (chain_id, block_number);

CREATE INDEX transaction_from_idx
    ON transaction (chain_id, from_address);

CREATE INDEX transaction_to_idx
    ON transaction (chain_id, to_address)
    WHERE to_address IS NOT NULL;

-- Partial index — canonical only, used by most application queries
CREATE INDEX transaction_canonical_from_idx
    ON transaction (chain_id, from_address, block_number)
    WHERE is_canonical = true;

CREATE INDEX transaction_canonical_to_idx
    ON transaction (chain_id, to_address, block_number)
    WHERE is_canonical = true AND to_address IS NOT NULL;

-- Partial index for contract deployments (to_address IS NULL)
CREATE INDEX transaction_deployment_idx
    ON transaction (chain_id, block_number)
    WHERE to_address IS NULL AND is_canonical = true;

-- Partial index for TRC10 transfers on Tron
CREATE INDEX transaction_trc10_idx
    ON transaction (chain_id, block_number)
    WHERE transaction_type = 3 AND is_canonical = true;


-- =============================================================================
-- RECEIPT
--
-- Column mapping:
--   Both chains:
--     status             → status (1=success, 0=fail)
--     contract_address   → contract_address (set on deployment)
--
--   EVM:
--     gasUsed             → execution_cost_used
--     cumulativeGasUsed   → cumulative_cost_used
--     effectiveGasPrice   → effective_cost_per_unit
--     logsBloom           → logs_bloom
--     root (pre-Byzantium)→ root
--     blobGasUsed         → meta->>'blob_gas_used'
--
--   Tron (from TransactionInfo):
--     energy_usage_total  → execution_cost_used
--     fee (TRX burned)    → total_fee
--     net_usage           → meta->>'net_usage'       (bandwidth consumed)
--     net_fee             → meta->>'net_fee'          (TRX burned for bandwidth)
--     energy_usage        → meta->>'energy_usage'
--     energy_fee          → meta->>'energy_fee'       (TRX burned for energy)
--     origin_energy_usage → meta->>'origin_energy_usage'
--     contract_result     → meta->>'contract_result'  (raw return bytes)
--     result string       → status (mapped: 'SUCCESS'→1, 'FAILED'→0)
-- =============================================================================
CREATE TABLE receipt (
    chain_id                integer     NOT NULL
                                REFERENCES chain (chain_id),
    transaction_hash        text        NOT NULL,
    block_hash              text        NOT NULL,
    block_number            bigint      NOT NULL,
    transaction_index       integer     NOT NULL,
    from_address            text        NOT NULL,
    to_address              text,

    -- Set when a new contract was deployed by this transaction
    contract_address        text,

    -- 1 = success, 0 = failure, null = pre-Byzantium EVM (no status field)
    status                  smallint,

    -- Unified cost tracking
    -- EVM:  gas_used
    -- Tron: energy_usage_total
    execution_cost_used     bigint,

    -- EVM only: cumulative gas used in block up to and including this tx
    cumulative_cost_used    bigint,

    -- EVM only: actual gas price paid (derived for EIP-1559 txns)
    effective_cost_per_unit bigint,

    -- Total native token fee paid (in smallest unit)
    -- EVM:  derived = gas_used × effective_gas_price (stored for convenience)
    -- Tron: fee field from TransactionInfo (TRX burned in sun)
    total_fee               numeric,

    -- EVM only: bloom filter for logs in this receipt
    logs_bloom              text,

    -- EVM pre-Byzantium only
    root                    text,

    -- Chain-specific overflow (see mapping above)
    meta                    jsonb,

    PRIMARY KEY (chain_id, transaction_hash),

    FOREIGN KEY (chain_id, transaction_hash)
        REFERENCES transaction (chain_id, hash)
);

CREATE INDEX receipt_block_idx
    ON receipt (chain_id, block_hash);

CREATE INDEX receipt_contract_address_idx
    ON receipt (chain_id, contract_address)
    WHERE contract_address IS NOT NULL;

CREATE INDEX receipt_status_idx
    ON receipt (chain_id, status, block_number);


-- =============================================================================
-- LOG
--
-- Structurally identical for EVM and Tron TRC20.
-- Both use keccak256(eventSignature) as topic[0].
-- Both ABI-encode non-indexed params in data.
-- Both left-pad 20-byte addresses to 32 bytes in topics.
--
-- Address encoding difference (handled in ETL, not schema):
--   EVM:  20-byte address, 12 zero bytes of left-padding
--         topic = 0x000000000000000000000000<20-byte-address>
--   Tron: 21-byte address (0x41 prefix + 20 bytes), 11 zero bytes of padding
--         topic = 0x00000000000000000000004<20-byte-address>
--         right(topic, 40) extracts same 20 bytes on both — consistent in SQL
--
-- TRC10 transfers do NOT appear here — see trc10_transfer table.
-- Native ETH/MATIC/TRX transfers do NOT appear here — see transaction.value.
-- =============================================================================
CREATE TABLE log (
    chain_id            integer     NOT NULL
                            REFERENCES chain (chain_id),
    id                  bigint      GENERATED ALWAYS AS IDENTITY,
    transaction_hash    text        NOT NULL,
    block_hash          text        NOT NULL,
    block_number        bigint      NOT NULL,
    transaction_index   integer     NOT NULL,
    log_index           integer     NOT NULL,

    -- Contract that emitted this log (0x-prefixed hex on both chains)
    address             text        NOT NULL,

    -- ABI-encoded non-indexed event parameters
    data                text,               -- 0x-prefixed

    -- True when a reorg removed this log from the canonical chain
    -- Always false on Tron (near-instant finality, deep reorgs impossible)
    removed             boolean     NOT NULL DEFAULT false,

    PRIMARY KEY (chain_id, id),

    CONSTRAINT log_unique_position
        UNIQUE (chain_id, block_hash, transaction_hash, log_index),

    FOREIGN KEY (chain_id, transaction_hash)
        REFERENCES transaction (chain_id, hash)
);

CREATE INDEX log_transaction_idx
    ON log (chain_id, transaction_hash);

CREATE INDEX log_block_number_idx
    ON log (chain_id, block_number);

-- Primary filter for any contract's events
CREATE INDEX log_address_block_idx
    ON log (chain_id, address, block_number);


-- =============================================================================
-- LOG_TOPIC
--
-- Identical structure for EVM and Tron TRC20 — no differences.
--
-- position 0 = keccak256(eventSignature)
--   ERC20/TRC20 Transfer:  0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef
--   ERC20/TRC20 Approval:  0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925
--   ERC721 Transfer:       0xddf252ad... (same sig, different contract type)
--
-- position 1-3 = indexed event parameters, left-padded to 32 bytes
--   For address params: '0x' || right(value, 40) recovers the 20-byte address
--   Works on both EVM and Tron despite the 0x41 prefix difference
-- =============================================================================
CREATE TABLE log_topic (
    chain_id    integer     NOT NULL
                    REFERENCES chain (chain_id),
    log_id      bigint      NOT NULL,
    position    smallint    NOT NULL
                    CHECK (position BETWEEN 0 AND 3),
    value       text        NOT NULL,   -- 0x-prefixed, 32 bytes

    PRIMARY KEY (chain_id, log_id, position),

    FOREIGN KEY (chain_id, log_id)
        REFERENCES log (chain_id, id)
);

-- Core filter: find all logs with a given event signature
CREATE INDEX log_topic_sig_idx
    ON log_topic (chain_id, value)
    WHERE position = 0;

-- Covering index for the full eth_getLogs pattern:
-- WHERE chain_id = ? AND position = 0 AND value = <sig>
-- AND log_id IN (SELECT id FROM log WHERE address = ?)
CREATE INDEX log_topic_position_value_idx
    ON log_topic (chain_id, position, value);


-- =============================================================================
-- INTERNAL_TRANSACTION
--
-- EVM:  sourced from debug_traceTransaction or trace_block (archive node only)
--       Captures sub-calls: CALL, DELEGATECALL, STATICCALL, CREATE, CREATE2
--       Essential for finding factory-deployed contracts and internal ETH moves
--
-- Tron: native in TransactionInfo.internal_transactions
--       Always available — no archive node required
--       Captures TRX moved between contracts during execution
--
-- call_type values:
--   EVM:  'call', 'delegatecall', 'staticcall', 'create', 'create2'
--   Tron: 'call', 'create' (Tron uses 'note' field for human description — in meta)
-- =============================================================================
CREATE TABLE internal_transaction (
    chain_id            integer     NOT NULL
                            REFERENCES chain (chain_id),
    id                  bigint      GENERATED ALWAYS AS IDENTITY,
    transaction_hash    text        NOT NULL,
    block_number        bigint      NOT NULL,

    -- Position within the parent transaction's call tree
    -- EVM:  traceAddress array encoded as string e.g. '[0,1,2]'
    -- Tron: sequential index
    trace_address       text        NOT NULL,

    from_address        text,
    to_address          text,

    -- Value transferred in native smallest unit (wei / sun)
    value               numeric     NOT NULL DEFAULT 0,

    -- Calldata for calls, deployment bytecode for creates
    input               text,

    -- 'call', 'delegatecall', 'staticcall', 'create', 'create2'
    call_type           text        NOT NULL,

    -- EVM only (null on Tron)
    gas                 bigint,
    gas_used            bigint,

    -- Return data (EVM) / output (Tron)
    output              text,

    -- 1 = success, 0 = reverted/failed
    status              smallint    NOT NULL DEFAULT 1,

    -- True if this internal tx was part of a reverted sub-call (Tron)
    rejected            boolean     NOT NULL DEFAULT false,

    -- Tron: { "note": "call", "extra": "..." }
    -- EVM:  { "error": "execution reverted", "revertReason": "..." }
    meta                jsonb,

    PRIMARY KEY (chain_id, id),

    FOREIGN KEY (chain_id, transaction_hash)
        REFERENCES transaction (chain_id, hash)
);

CREATE INDEX internal_tx_transaction_idx
    ON internal_transaction (chain_id, transaction_hash);

CREATE INDEX internal_tx_from_idx
    ON internal_transaction (chain_id, from_address)
    WHERE from_address IS NOT NULL;

CREATE INDEX internal_tx_to_idx
    ON internal_transaction (chain_id, to_address)
    WHERE to_address IS NOT NULL;

-- Index for finding all contract creations (factory deployments)
CREATE INDEX internal_tx_create_idx
    ON internal_transaction (chain_id, block_number)
    WHERE call_type IN ('create', 'create2') AND status = 1;


-- =============================================================================
-- TRC10_TOKEN
--
-- Tron protocol-level token registry. TRC10 tokens are not smart contracts —
-- they have no address, no ABI, and no logs. They are identified by a
-- protocol-assigned integer token_id and managed entirely via Tron node APIs:
--   GET /wallet/getassetissuebyid?value=<token_id>
--
-- This table is populated via an enrichment job, not from chain scanning.
-- EVM chains have no equivalent — all tokens on EVM are smart contracts.
-- =============================================================================
CREATE TABLE trc10_token (
    chain_id            integer         NOT NULL
                            REFERENCES chain (chain_id),

    -- Protocol-assigned integer, stored as text to avoid numeric precision loss
    -- e.g. '1000001' for the original BTT token
    token_id            text            NOT NULL,

    name                text,           -- e.g. 'BitTorrent'
    abbreviation        text,           -- e.g. 'BTT'
    description         text,
    url                 text,

    -- Token supply (in smallest unit)
    total_supply        numeric,
    frozen_supply       numeric,        -- tokens locked at issuance

    -- Decimal places (Tron calls this 'precision')
    precision           smallint        NOT NULL DEFAULT 0,

    -- The address that created/issued this token
    issuer_address      text,

    -- Block when this token was created
    created_block       bigint,

    -- Optional built-in exchange rate to TRX set at issuance
    -- token_ratio tokens = trx_ratio TRX
    trx_ratio           bigint,
    token_ratio         bigint,

    -- Token lifecycle
    start_time_us       bigint,         -- when token sale/distribution started
    end_time_us         bigint,         -- when token sale/distribution ended
    is_active           boolean         NOT NULL DEFAULT true,

    -- Raw API response for any fields not captured above
    meta                jsonb,

    PRIMARY KEY (chain_id, token_id),

    -- chain_id must be a Tron chain
    CONSTRAINT trc10_token_tron_only CHECK (chain_id >= 1000)
);

CREATE INDEX trc10_token_issuer_idx
    ON trc10_token (chain_id, issuer_address)
    WHERE issuer_address IS NOT NULL;

CREATE INDEX trc10_token_abbreviation_idx
    ON trc10_token USING gin (abbreviation gin_trgm_ops);


-- =============================================================================
-- TRC10_TRANSFER
--
-- TRC10 transfers travel at the TRANSACTION layer, not the log layer.
-- They are sourced from transactions where transaction_type = 3
-- (TransferAssetContract) and extracted into this dedicated table for
-- analytical parity with ERC20/TRC20 log-based transfers.
--
-- This table has no EVM equivalent because on EVM all token transfers
-- go through smart contracts and emit logs. TRC10 is unique to Tron.
--
-- Each row corresponds to exactly one transaction row (transaction_type = 3).
-- The transaction row is kept for completeness; this table is for analytics.
-- =============================================================================
CREATE TABLE trc10_transfer (
    chain_id            integer     NOT NULL
                            REFERENCES chain (chain_id),
    id                  bigint      GENERATED ALWAYS AS IDENTITY,

    -- Source transaction
    transaction_hash    text        NOT NULL,
    block_hash          text        NOT NULL,
    block_number        bigint      NOT NULL,
    transaction_index   integer     NOT NULL,

    -- Timestamp (denormalized from block for query performance)
    timestamp_us        bigint      NOT NULL,

    from_address        text        NOT NULL,
    to_address          text        NOT NULL,

    -- Token identifier
    token_id            text        NOT NULL,   -- references trc10_token.token_id

    -- Transfer amount in token smallest unit
    -- Divide by 10^trc10_token.precision for human-readable amount
    raw_amount          numeric     NOT NULL,

    PRIMARY KEY (chain_id, id),

    CONSTRAINT trc10_transfer_unique
        UNIQUE (chain_id, transaction_hash),

    FOREIGN KEY (chain_id, transaction_hash)
        REFERENCES transaction (chain_id, hash),

    FOREIGN KEY (chain_id, token_id)
        REFERENCES trc10_token (chain_id, token_id)
);

CREATE INDEX trc10_transfer_block_idx
    ON trc10_transfer (chain_id, block_number);

CREATE INDEX trc10_transfer_token_idx
    ON trc10_transfer (chain_id, token_id, block_number);

CREATE INDEX trc10_transfer_from_idx
    ON trc10_transfer (chain_id, from_address, block_number);

CREATE INDEX trc10_transfer_to_idx
    ON trc10_transfer (chain_id, to_address, block_number);


-- =============================================================================
-- CONTRACT
--
-- Enrichment table — one row per deployed contract per chain.
-- Populated two ways:
--   1. Automatically: when receipt.contract_address IS NOT NULL (direct deploy)
--                     or when internal_transaction.call_type IN ('create','create2')
--   2. Manually: ABI registration for contracts you want to decode
--
-- Tron TRC10 tokens are NOT in this table — they are not contracts.
-- Tron TRC20 tokens ARE in this table — they are contracts.
-- =============================================================================
CREATE TABLE contract (
    chain_id            integer     NOT NULL
                            REFERENCES chain (chain_id),

    -- 0x-prefixed hex, 20 bytes (both EVM and Tron)
    address             text        NOT NULL,

    created_block       bigint,
    created_tx_hash     text,
    creator_address     text,

    -- True if this is a proxy contract (EIP-1967, Transparent, UUPS, etc.)
    is_proxy            boolean     NOT NULL DEFAULT false,
    -- For proxy contracts: the current implementation contract address
    implementation      text,

    -- Contract ABI as JSON array — used by decoding layer
    -- Populated from Etherscan verification, manual registration, or 4byte.directory
    abi                 jsonb,

    -- Human-readable metadata (from ABI or token registry)
    name                text,
    symbol              text,           -- ERC20/TRC20/ERC721 only
    decimals            smallint,       -- ERC20/TRC20 only

    -- Token standard classification
    -- 'ERC20', 'ERC721', 'ERC1155', 'TRC20', 'TRC721', 'unknown', null
    token_standard      text,

    -- Contract bytecode (deployment bytecode, not runtime)
    bytecode            text,           -- 0x-prefixed

    -- Chain-specific overflow
    -- Tron: { "origin_address": "T...", "consume_user_resource_percent": 100,
    --         "origin_energy_limit": 10000000 }
    -- EVM:  { "verified_on_etherscan": true, "compiler_version": "0.8.19" }
    meta                jsonb,

    PRIMARY KEY (chain_id, address)
);

CREATE INDEX contract_created_block_idx
    ON contract (chain_id, created_block)
    WHERE created_block IS NOT NULL;

CREATE INDEX contract_token_standard_idx
    ON contract (chain_id, token_standard)
    WHERE token_standard IS NOT NULL;

-- Text search on contract name and symbol
CREATE INDEX contract_name_trgm_idx
    ON contract USING gin (name gin_trgm_ops)
    WHERE name IS NOT NULL;

CREATE INDEX contract_symbol_trgm_idx
    ON contract USING gin (symbol gin_trgm_ops)
    WHERE symbol IS NOT NULL;


-- =============================================================================
-- FUNCTION_SELECTOR
--
-- 4-byte selector registry for transaction intent classification.
-- EVM only — Tron has protocol-typed transactions.
--
-- Populated from:
--   - 4byte.directory (bulk import)
--   - Contract ABI registration (highest confidence)
--   - Runtime discovery during ETL
--
-- A selector may have multiple candidate signatures (hash collisions are rare
-- but real). The most_likely flag marks the best candidate when ambiguous.
-- =============================================================================
CREATE TABLE function_selector (
    selector        text        NOT NULL,   -- '0xa9059cbb' (8 hex chars + 0x)
    signature       text        NOT NULL,   -- 'transfer(address,uint256)'
    most_likely     boolean     NOT NULL DEFAULT true,
    abi_fragment    jsonb,                  -- full ABI input/output types

    PRIMARY KEY (selector, signature)
);

CREATE INDEX function_selector_sig_idx
    ON function_selector (signature);

-- Common ERC20 selectors
INSERT INTO function_selector (selector, signature) VALUES
    ('0xa9059cbb', 'transfer(address,uint256)'),
    ('0x23b872dd', 'transferFrom(address,address,uint256)'),
    ('0x095ea7b3', 'approve(address,uint256)'),
    ('0x70a08231', 'balanceOf(address)'),
    ('0x18160ddd', 'totalSupply()'),
    ('0x06fdde03', 'name()'),
    ('0x95d89b41', 'symbol()'),
    ('0x313ce567', 'decimals()'),
    -- WETH
    ('0xd0e30db0', 'deposit()'),
    ('0x2e1a7d4d', 'withdraw(uint256)'),
    -- Uniswap V2
    ('0x38ed1739', 'swapExactTokensForTokens(uint256,uint256,address[],address,uint256)'),
    ('0x7ff36ab5', 'swapExactETHForTokens(uint256,address[],address,uint256)'),
    ('0x18cbafe5', 'swapExactTokensForETH(uint256,uint256,address[],address,uint256)'),
    ('0xe8e33700', 'addLiquidity(address,address,uint256,uint256,uint256,uint256,address,uint256)'),
    ('0xbaa2abde', 'removeLiquidity(address,address,uint256,uint256,uint256,address,uint256)'),
    -- Uniswap V3
    ('0x5ae401dc', 'multicall(uint256,bytes[])'),
    ('0xac9650d8', 'multicall(bytes[])'),
    ('0x414bf389', 'exactInputSingle((address,address,uint24,address,uint256,uint256,uint256,uint160))'),
    ('0xc04b8d59', 'exactInput((bytes,address,uint256,uint256,uint256))'),
    -- ERC721
    ('0x42842e0e', 'safeTransferFrom(address,address,uint256)'),
    ('0xb88d4fde', 'safeTransferFrom(address,address,uint256,bytes)'),
    ('0xa22cb465', 'setApprovalForAll(address,bool)'),
    ('0x6352211e', 'ownerOf(uint256)'),
    -- ETH2 staking deposit contract
    ('0x22895118', 'deposit(bytes,bytes,bytes,bytes32)'),
    -- Gnosis Safe
    ('0x6a761202', 'execTransaction(address,uint256,bytes,uint8,uint256,uint256,uint256,address,address,bytes)');


-- =============================================================================
-- INDEXER_CHECKPOINT
--
-- Tracks which block ranges have been fully indexed per chain.
-- Used by the ETL process to resume after restarts and detect gaps.
-- A gap between two checkpoint rows = unindexed range.
-- =============================================================================
CREATE TABLE indexer_checkpoint (
    chain_id        integer         NOT NULL
                        REFERENCES chain (chain_id),
    from_block      bigint          NOT NULL,
    to_block        bigint          NOT NULL,
    indexed_at      timestamptz     NOT NULL DEFAULT now(),

    -- 'complete'  = all tables populated for this range
    -- 'partial'   = some tables populated (e.g. logs but not traces)
    -- 'failed'    = indexing failed, range should be retried
    status          text            NOT NULL DEFAULT 'complete'
                        CHECK (status IN ('complete', 'partial', 'failed')),

    -- Which data types were indexed in this pass
    -- e.g. '{"blocks": true, "transactions": true, "logs": true, "traces": false}'
    coverage        jsonb,

    PRIMARY KEY (chain_id, from_block, to_block),

    CONSTRAINT checkpoint_range_valid
        CHECK (to_block >= from_block)
);

CREATE INDEX checkpoint_chain_to_block_idx
    ON indexer_checkpoint (chain_id, to_block DESC);


-- =============================================================================
-- REORG_EVENT
--
-- Audit log of every chain reorganisation detected.
-- When a reorg is detected at depth D from block N:
--   1. Insert a reorg_event row
--   2. Set block.is_canonical = false for the orphaned blocks
--   3. Set transaction.is_canonical = false for orphaned transactions
--   4. Set log.removed = true for logs from orphaned transactions
--   5. Re-index the new canonical blocks/transactions/logs
--
-- Tron reorgs are extremely rare (near-instant BFT finality) but the
-- table handles them uniformly across chains.
-- =============================================================================
CREATE TABLE reorg_event (
    id                  bigint          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    chain_id            integer         NOT NULL
                            REFERENCES chain (chain_id),
    detected_at         timestamptz     NOT NULL DEFAULT now(),

    -- First block number where the canonical chain diverged
    forked_at_block     bigint          NOT NULL,

    -- How many blocks were reorganised
    depth               integer         NOT NULL CHECK (depth > 0),

    old_head_hash       text            NOT NULL,
    new_head_hash       text            NOT NULL,

    -- Set after recovery is complete
    resolved_at         timestamptz
);

CREATE INDEX reorg_event_chain_idx
    ON reorg_event (chain_id, forked_at_block DESC);


-- =============================================================================
-- HELPER VIEWS
--
-- These views are NOT materialized — they are stable query building blocks
-- for the decoding layer above. They filter canonical-only, join block
-- timestamps, and pivot log topics into columns.
-- =============================================================================

-- -------------------------------------------------------
-- v_canonical_block: canonical blocks with derived fields
-- -------------------------------------------------------
CREATE VIEW v_canonical_block AS
SELECT
    b.chain_id,
    b.number,
    b.hash,
    b.timestamp_us,
    -- Convenience: timestamp as timestamptz
    to_timestamp(b.timestamp_us / 1000000.0)    AS block_time,
    b.block_producer,
    b.gas_limit,
    b.gas_used,
    b.base_fee_per_gas,
    b.transaction_count,
    b.size_bytes
FROM block b
WHERE b.is_canonical = true;


-- -------------------------------------------------------
-- v_canonical_transaction: canonical txns with block time
-- and receipt fields joined for convenience
-- -------------------------------------------------------
CREATE VIEW v_canonical_transaction AS
SELECT
    t.chain_id,
    t.hash,
    t.block_number,
    t.block_hash,
    t.transaction_index,
    to_timestamp(b.timestamp_us / 1000000.0)    AS block_time,
    t.from_address,
    t.to_address,
    t.value,
    t.input,
    t.nonce,
    t.cost_limit,
    t.cost_per_unit,
    t.max_cost_per_unit,
    t.transaction_type,
    -- From receipt
    r.status,
    r.execution_cost_used,
    r.effective_cost_per_unit,
    r.total_fee,
    r.contract_address          AS created_contract_address,
    t.meta                      AS tx_meta,
    r.meta                      AS receipt_meta
FROM transaction t
JOIN block b
    ON  b.chain_id      = t.chain_id
    AND b.hash          = t.block_hash
    AND b.is_canonical  = true
LEFT JOIN receipt r
    ON  r.chain_id          = t.chain_id
    AND r.transaction_hash  = t.hash
WHERE t.is_canonical = true;


-- -------------------------------------------------------
-- v_canonical_log: canonical, non-removed logs with block time
-- Base view for all log-based decoding (ERC20, TRC20, etc.)
-- -------------------------------------------------------
CREATE VIEW v_canonical_log AS
SELECT
    l.chain_id,
    l.id                                        AS log_id,
    l.block_number,
    l.block_hash,
    to_timestamp(b.timestamp_us / 1000000.0)    AS block_time,
    l.transaction_hash,
    l.transaction_index,
    l.log_index,
    l.address                                   AS contract_address,
    l.data
FROM log l
JOIN transaction t
    ON  t.chain_id      = l.chain_id
    AND t.hash          = l.transaction_hash
    AND t.is_canonical  = true
JOIN block b
    ON  b.chain_id      = l.chain_id
    AND b.hash          = l.block_hash
    AND b.is_canonical  = true
WHERE l.removed = false;


-- -------------------------------------------------------
-- v_log_with_topics: canonical logs with all 4 topics
-- pivoted into columns. This is the primary input for all
-- decoded event materialized views.
--
-- topic0 = event signature hash (always present for named events)
-- topic1-3 = indexed parameters (LEFT JOIN = null if not present)
-- -------------------------------------------------------
CREATE VIEW v_log_with_topics AS
SELECT
    l.chain_id,
    l.log_id,
    l.block_number,
    l.block_time,
    l.transaction_hash,
    l.transaction_index,
    l.log_index,
    l.contract_address,
    l.data,
    t0.value    AS topic0,
    t1.value    AS topic1,
    t2.value    AS topic2,
    t3.value    AS topic3
FROM v_canonical_log l
LEFT JOIN log_topic t0
    ON  t0.chain_id = l.chain_id
    AND t0.log_id   = l.log_id
    AND t0.position = 0
LEFT JOIN log_topic t1
    ON  t1.chain_id = l.chain_id
    AND t1.log_id   = l.log_id
    AND t1.position = 1
LEFT JOIN log_topic t2
    ON  t2.chain_id = l.chain_id
    AND t2.log_id   = l.log_id
    AND t2.position = 2
LEFT JOIN log_topic t3
    ON  t3.chain_id = l.chain_id
    AND t3.log_id   = l.log_id
    AND t3.position = 3;


-- -------------------------------------------------------
-- v_classified_transaction: canonical transactions with
-- semantic type classification using function selectors
-- -------------------------------------------------------
CREATE VIEW v_classified_transaction AS
SELECT
    ct.*,
    -- 4-byte selector (null for native transfers and deployments)
    CASE
        WHEN length(ct.input) >= 10
        THEN left(ct.input, 10)
    END                                         AS selector,
    -- Human-readable function name from registry
    fs.signature                                AS function_name,
    -- Semantic type classification
    CASE
        WHEN ct.to_address IS NULL
            THEN 'contract_deployment'
        WHEN ct.input IS NULL OR ct.input = '0x'
            THEN 'native_transfer'
        WHEN left(ct.input, 10) IN (
            '0xa9059cbb',   -- transfer
            '0x23b872dd',   -- transferFrom
            '0x095ea7b3'    -- approve
        )   THEN 'erc20'
        WHEN left(ct.input, 10) IN (
            '0x38ed1739', '0x7ff36ab5', '0x18cbafe5',   -- Uniswap V2 swaps
            '0x5ae401dc', '0xac9650d8',                  -- Uniswap V3 multicall
            '0x414bf389', '0xc04b8d59'                   -- Uniswap V3 exactInput
        )   THEN 'dex_swap'
        WHEN left(ct.input, 10) IN (
            '0xd0e30db0',   -- WETH deposit
            '0x2e1a7d4d'    -- WETH withdraw
        )   THEN 'wrap_unwrap'
        WHEN left(ct.input, 10) IN (
            '0xe8e33700', '0xbaa2abde'   -- add/remove liquidity
        )   THEN 'liquidity'
        WHEN left(ct.input, 10) IN (
            '0x42842e0e', '0xb88d4fde',  -- ERC721 safeTransferFrom
            '0xa22cb465'                  -- ERC721 setApprovalForAll
        )   THEN 'erc721'
        WHEN left(ct.input, 10) = '0x22895118'
            THEN 'eth2_staking'
        WHEN left(ct.input, 10) = '0x6a761202'
            THEN 'multisig_execution'
        ELSE 'other_contract_call'
    END                                         AS semantic_type
FROM v_canonical_transaction ct
LEFT JOIN function_selector fs
    ON  fs.selector     = left(ct.input, 10)
    AND fs.most_likely  = true;


-- =============================================================================
-- DECODED MATERIALIZED VIEWS
--
-- These sit on top of v_log_with_topics and provide fast, typed access
-- to the most common event types across all supported chains.
-- Refresh nightly (or use TimescaleDB continuous aggregates for near-realtime).
--
-- Address extraction note:
--   Both EVM and Tron: '0x' || right(topicN, 40)
--   This strips the left-padding on both chains correctly.
--   The 0x41 Tron network prefix byte occupies a middle position and
--   right(topic, 40) takes only the final 20 bytes on both chains.
-- =============================================================================

-- -------------------------------------------------------
-- fact_erc20_transfer (also covers TRC20)
-- Works on chain_id 1 (ETH), 137 (Polygon), 1000 (Tron)
-- -------------------------------------------------------
CREATE MATERIALIZED VIEW fact_erc20_transfer AS
SELECT
    l.chain_id,
    l.log_id,
    l.block_number,
    l.block_time,
    l.transaction_hash,
    l.log_index,
    l.contract_address                              AS token_address,
    c.symbol                                        AS token_symbol,
    c.decimals                                      AS token_decimals,

    -- Recover 20-byte addresses by stripping left-padding
    ('0x' || right(l.topic1, 40))                   AS from_address,
    ('0x' || right(l.topic2, 40))                   AS to_address,

    -- Decode raw uint256 amount from data field
    -- data is a 0x-prefixed 32-byte hex string for a single uint256
    ('x' || lpad(ltrim(l.data, '0x'), 64, '0'))::bit(256)::numeric
                                                    AS raw_amount,

    -- Human-readable amount (null if decimals not yet known)
    CASE WHEN c.decimals IS NOT NULL THEN
        ('x' || lpad(ltrim(l.data, '0x'), 64, '0'))::bit(256)::numeric
        / power(10, c.decimals::numeric)
    END                                             AS amount,

    -- Transfer type classification
    CASE
        -- Mint: from the zero address
        WHEN right(l.topic1, 40) = lpad('0', 40, '0')
            THEN 'mint'
        -- Burn: to the zero address
        WHEN right(l.topic2, 40) = lpad('0', 40, '0')
            THEN 'burn'
        -- Burn: to the dead address (common alternative burn destination)
        WHEN right(l.topic2, 40) = 'dead000000000000000000000000000000000000'
            THEN 'burn'
        ELSE 'transfer'
    END                                             AS transfer_type

FROM v_log_with_topics l
LEFT JOIN contract c
    ON  c.chain_id  = l.chain_id
    AND c.address   = l.contract_address

-- ERC20 and TRC20 Transfer event signature (identical on both chains)
WHERE l.topic0 = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
  AND l.topic1 IS NOT NULL   -- must have from address
  AND l.topic2 IS NOT NULL   -- must have to address
  AND l.data IS NOT NULL     -- must have amount

WITH NO DATA;

CREATE UNIQUE INDEX fact_erc20_transfer_pk_idx
    ON fact_erc20_transfer (chain_id, log_id);

CREATE INDEX fact_erc20_transfer_token_time_idx
    ON fact_erc20_transfer (chain_id, token_address, block_time);

CREATE INDEX fact_erc20_transfer_from_time_idx
    ON fact_erc20_transfer (chain_id, from_address, block_time);

CREATE INDEX fact_erc20_transfer_to_time_idx
    ON fact_erc20_transfer (chain_id, to_address, block_time);

CREATE INDEX fact_erc20_transfer_type_idx
    ON fact_erc20_transfer (chain_id, transfer_type, block_time);


-- -------------------------------------------------------
-- fact_erc20_approval
-- -------------------------------------------------------
CREATE MATERIALIZED VIEW fact_erc20_approval AS
SELECT
    l.chain_id,
    l.log_id,
    l.block_number,
    l.block_time,
    l.transaction_hash,
    l.contract_address                              AS token_address,
    c.symbol                                        AS token_symbol,
    ('0x' || right(l.topic1, 40))                   AS owner_address,
    ('0x' || right(l.topic2, 40))                   AS spender_address,
    ('x' || lpad(ltrim(l.data, '0x'), 64, '0'))::bit(256)::numeric
                                                    AS raw_amount,
    -- Flag unlimited approvals (max uint256)
    -- Common pattern for DEX routers — useful for risk analysis
    CASE
        WHEN l.data = '0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff'
        THEN true
        ELSE false
    END                                             AS is_unlimited
FROM v_log_with_topics l
LEFT JOIN contract c
    ON  c.chain_id  = l.chain_id
    AND c.address   = l.contract_address
WHERE l.topic0 = '0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925'
WITH NO DATA;

CREATE UNIQUE INDEX fact_erc20_approval_pk_idx
    ON fact_erc20_approval (chain_id, log_id);

CREATE INDEX fact_erc20_approval_token_idx
    ON fact_erc20_approval (chain_id, token_address, block_time);

CREATE INDEX fact_erc20_approval_owner_idx
    ON fact_erc20_approval (chain_id, owner_address, block_time);

CREATE INDEX fact_erc20_approval_spender_idx
    ON fact_erc20_approval (chain_id, spender_address, block_time);


-- -------------------------------------------------------
-- fact_trc10_transfer (Tron only)
-- Surfaces TRC10 activity at the same query level as
-- fact_erc20_transfer so all token transfers can be
-- UNIONed without caring which layer they came from.
-- -------------------------------------------------------
CREATE MATERIALIZED VIEW fact_trc10_transfer AS
SELECT
    tt.chain_id,
    tt.id,
    tt.block_number,
    to_timestamp(b.timestamp_us / 1000000.0)        AS block_time,
    tt.transaction_hash,
    tt.token_id,
    tok.abbreviation                                AS token_symbol,
    tok.precision                                   AS token_decimals,
    tt.from_address,
    tt.to_address,
    tt.raw_amount,
    CASE WHEN tok.precision IS NOT NULL THEN
        tt.raw_amount / power(10, tok.precision::numeric)
    END                                             AS amount,
    'transfer'::text                                AS transfer_type
FROM trc10_transfer tt
JOIN block b
    ON  b.chain_id  = tt.chain_id
    AND b.hash      = tt.block_hash
    AND b.is_canonical = true
LEFT JOIN trc10_token tok
    ON  tok.chain_id = tt.chain_id
    AND tok.token_id = tt.token_id
WITH NO DATA;

CREATE UNIQUE INDEX fact_trc10_transfer_pk_idx
    ON fact_trc10_transfer (chain_id, id);

CREATE INDEX fact_trc10_transfer_token_time_idx
    ON fact_trc10_transfer (chain_id, token_id, block_time);

CREATE INDEX fact_trc10_transfer_from_time_idx
    ON fact_trc10_transfer (chain_id, from_address, block_time);

CREATE INDEX fact_trc10_transfer_to_time_idx
    ON fact_trc10_transfer (chain_id, to_address, block_time);


-- =============================================================================
-- UNIFIED TOKEN TRANSFER VIEW
--
-- Single query surface for ALL token transfers across ALL chains and ALL
-- token standards. Replaces the need to know which layer a token lives on.
--
-- Token type discriminator:
--   'erc20'  = Ethereum/Polygon ERC20 (from logs)
--   'trc20'  = Tron TRC20 (from logs — identical log structure to ERC20)
--   'trc10'  = Tron TRC10 (from transactions — no logs)
--
-- NOTE: Native currency transfers (ETH, MATIC, TRX) are NOT included here.
-- Query v_canonical_transaction WHERE value > 0 for those.
-- =============================================================================
CREATE VIEW v_all_token_transfers AS

    -- ERC20 (Ethereum and Polygon)
    SELECT
        chain_id,
        log_id::text                AS transfer_id,
        block_number,
        block_time,
        transaction_hash,
        token_address,
        token_symbol,
        token_decimals,
        from_address,
        to_address,
        raw_amount,
        amount,
        transfer_type,
        CASE chain_id
            WHEN 1   THEN 'erc20'
            WHEN 137 THEN 'erc20'
            ELSE          'erc20'
        END                         AS token_standard
    FROM fact_erc20_transfer
    WHERE chain_id IN (1, 137)      -- EVM chains only

UNION ALL

    -- TRC20 (Tron) — same fact table, different chain_id
    SELECT
        chain_id,
        log_id::text                AS transfer_id,
        block_number,
        block_time,
        transaction_hash,
        token_address,
        token_symbol,
        token_decimals,
        from_address,
        to_address,
        raw_amount,
        amount,
        transfer_type,
        'trc20'                     AS token_standard
    FROM fact_erc20_transfer
    WHERE chain_id = 1000           -- Tron only

UNION ALL

    -- TRC10 (Tron) — transaction layer, no logs
    SELECT
        chain_id,
        id::text                    AS transfer_id,
        block_number,
        block_time,
        transaction_hash,
        token_id                    AS token_address,
        token_symbol,
        token_decimals,
        from_address,
        to_address,
        raw_amount,
        amount,
        transfer_type,
        'trc10'                     AS token_standard
    FROM fact_trc10_transfer;


-- =============================================================================
-- REFRESH SCHEDULE
--
-- Run these after your nightly ETL completes.
-- Add CONCURRENTLY once the unique indexes are in place.
--
-- REFRESH MATERIALIZED VIEW CONCURRENTLY fact_erc20_transfer;
-- REFRESH MATERIALIZED VIEW CONCURRENTLY fact_erc20_approval;
-- REFRESH MATERIALIZED VIEW CONCURRENTLY fact_trc10_transfer;
--
-- Or with pg_cron:
-- SELECT cron.schedule('refresh-decoded-views', '0 2 * * *', $$
--   REFRESH MATERIALIZED VIEW CONCURRENTLY fact_erc20_transfer;
--   REFRESH MATERIALIZED VIEW CONCURRENTLY fact_erc20_approval;
--   REFRESH MATERIALIZED VIEW CONCURRENTLY fact_trc10_transfer;
-- $$);
-- =============================================================================


-- =============================================================================
-- EXAMPLE QUERIES
-- =============================================================================

-- All USDT transfers on all chains in the last 7 days:
-- SELECT chain_id, token_standard, count(*), sum(amount)
-- FROM v_all_token_transfers
-- WHERE token_symbol = 'USDT'
--   AND block_time > now() - interval '7 days'
-- GROUP BY 1, 2
-- ORDER BY 1, 2;

-- How many ERC20 tokens exist on Ethereum (ever emitted a Transfer):
-- SELECT count(DISTINCT token_address)
-- FROM fact_erc20_transfer
-- WHERE chain_id = 1;

-- Top 10 TRC10 tokens by transfer count:
-- SELECT token_id, token_symbol, count(*) AS transfers
-- FROM fact_trc10_transfer
-- WHERE chain_id = 1000
-- GROUP BY 1, 2
-- ORDER BY 3 DESC
-- LIMIT 10;

-- All token transfers TO a specific address across all chains and standards:
-- SELECT *
-- FROM v_all_token_transfers
-- WHERE to_address = '0xYourAddressHere'
-- ORDER BY block_time DESC
-- LIMIT 100;

-- Contract deployment count by chain (direct deployments only):
-- SELECT chain_id, count(*) AS contracts_deployed
-- FROM receipt
-- WHERE contract_address IS NOT NULL
-- GROUP BY 1;

-- Classify transactions by semantic type (EVM chains only):
-- SELECT semantic_type, count(*), sum(total_fee) AS fees_paid
-- FROM v_classified_transaction
-- WHERE chain_id = 1
--   AND block_time > now() - interval '1 day'
-- GROUP BY 1
-- ORDER BY 2 DESC;
