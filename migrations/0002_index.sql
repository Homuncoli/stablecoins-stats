begin;
CREATE INDEX idx_addresses_addr ON addresses(addr);
CREATE INDEX idx_tokens_contract_addr ON tokens(contract_addr);
CREATE INDEX idx_tokens_asset_id ON tokens(asset_id);
commit;