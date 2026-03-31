DROP PROCEDURE IF EXISTS insert_transfer(bigint, smallint, transfer_type, bytea, bytea, bigint, bytea, bytea, bool);
CREATE OR REPLACE PROCEDURE insert_transfer(transaction bigint, index smallint, transfer_t transfer_type, asset_id bytea, contract_addr_i bytea, value bigint, from_addr bytea, to_addr bytea, rejected bool) 
AS $$
DECLARE
    token_t token_type;
    token_id INT;
    from_addr_id INT;
    to_addr_id INT;
    contract_id INT;
BEGIN
    IF contract_addr_i IS NOT NULL THEN
        token_t := 'TRC20';
        INSERT INTO addresses (addr, addr_t)
            VALUES (contract_addr_i, 'Contract')
            ON CONFLICT (addr) DO NOTHING;
        SELECT id INTO contract_id FROM addresses WHERE addr = contract_addr_i;
    ELSIF asset_id IS NOT NULL THEN
        token_t := 'TRC10';
        contract_id := NULL;
    ELSE
        token_t := 'TRX';
        contract_id := NULL;
    END IF;
    
    INSERT INTO token (asset_name, contract_addr, token_t)
        VALUES (asset_id, contract_id, token_t)
        ON CONFLICT (asset_name) DO NOTHING;
    SELECT id INTO token_id FROM token WHERE asset_name = asset_id OR contract_addr = contract_id;

    INSERT INTO addresses (addr, addr_t)
        VALUES (from_addr, 'EOA')
        ON CONFLICT (addr) DO NOTHING;
    SELECT id INTO from_addr_id FROM addresses WHERE addr = from_addr;

    INSERT INTO addresses (addr, addr_t)
        VALUES (to_addr, 'EOA')
        ON CONFLICT (addr) DO NOTHING;
    SELECT id INTO to_addr_id FROM addresses WHERE addr = to_addr;

    INSERT INTO transfers (transaction, index, transfer_t, token, value, contract, to_addr, from_addr, rejected)
        VALUES (transaction, index, transfer_t, token_id, value, contract_id, to_addr_id, from_addr_id, rejected)
        ON CONFLICT DO NOTHING;
END;
$$ LANGUAGE plpgsql;
