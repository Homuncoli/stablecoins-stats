begin;
CREATE INDEX idx_addresses_addr ON addresses(addr);
CREATE INDEX idx_tokens_contract_addr ON tokens(contract_addr);
CREATE INDEX idx_tokens_asset_id ON tokens(asset_id);
CREATE INDEX idx_transactions_block ON transactions((id / 1000));
CREATE INDEX idx_transfers_block ON transfers((transaction / 1000));

CREATE OR REPLACE FUNCTION from_uint256(value_hi bigint, value_lo bigint)
RETURNS numeric AS $$
    SELECT 
        (CASE WHEN value_hi < 0 
              THEN value_hi::numeric + 2::numeric^63 * 2::numeric^64
              ELSE value_hi::numeric 
         END) * 2::numeric^128

        + (CASE WHEN value_lo < 0 
                THEN value_lo::numeric + 2::numeric^128 
                ELSE value_lo::numeric 
           END)
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;
commit;