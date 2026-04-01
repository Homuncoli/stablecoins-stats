DROP PROCEDURE IF EXISTS insert_transfer(bigint, smallint, transfer_type, bytea, bytea, bigint, bytea, bytea, bool);
CREATE OR REPLACE PROCEDURE insert_transfer(
    transaction bigint, index smallint, transfer_t transfer_type,
    asset_id bytea, contract_addr_i bytea, value bigint,
    from_addr bytea, to_addr bytea, rejected bool
)
AS $$
DECLARE
    token_t      token_type;
    token_id     INT;
    from_addr_id INT;
    to_addr_id   INT;
    contract_id  INT;
BEGIN
    -- 1. Resolve token type and contract address
    IF contract_addr_i IS NOT NULL THEN
        token_t := 'TRC20';
        SELECT id INTO contract_id FROM addresses WHERE addr = contract_addr_i;
        IF NOT FOUND THEN
            INSERT INTO addresses (addr, addr_t) VALUES (contract_addr_i, 'Contract')
            ON CONFLICT (addr) DO NOTHING
            RETURNING id INTO contract_id;

            IF contract_id IS NULL THEN
                SELECT id INTO contract_id FROM addresses WHERE addr = contract_addr_i;
            END IF;
        END IF;
    ELSIF asset_id IS NOT NULL THEN
        token_t := 'TRC10';
        contract_id := NULL;
    ELSE
        token_t := 'TRX';
        contract_id := NULL;
    END IF;

    -- 2. Get or create token
    SELECT t.id INTO token_id FROM token t LEFT JOIN addresses a ON t.contract_addr = a.id WHERE asset_name = asset_id OR a.addr = contract_addr_i;
    IF NOT FOUND THEN
        INSERT INTO token (asset_name, contract_addr, token_t) VALUES (asset_id, contract_id, token_t)
        RETURNING id INTO token_id;

        IF token_id IS NULL THEN
            SELECT t.id INTO token_id FROM token t LEFT JOIN addresses a ON t.contract_addr = a.id WHERE asset_name = asset_id OR a.addr = contract_addr_i;
        END IF;
    END IF;

    -- 3. Get or create from address
    SELECT id INTO from_addr_id FROM addresses WHERE addr = from_addr;
    IF NOT FOUND THEN
        INSERT INTO addresses (addr, addr_t) VALUES (from_addr, 'EOA')
        ON CONFLICT (addr) DO NOTHING
        RETURNING id INTO from_addr_id;

        IF from_addr_id IS NULL THEN
            SELECT id INTO from_addr_id FROM addresses WHERE addr = from_addr;
        END IF;
    END IF;

    -- 4. Get or create to address
    SELECT id INTO to_addr_id FROM addresses WHERE addr = to_addr;
    IF NOT FOUND THEN
        INSERT INTO addresses (addr, addr_t) VALUES (to_addr, 'EOA')
        ON CONFLICT (addr) DO NOTHING
        RETURNING id INTO to_addr_id;
    
        IF to_addr_id IS NULL THEN
            SELECT id INTO to_addr_id FROM addresses WHERE addr = to_addr;
        END IF;
    END IF;

    -- 5. Insert transfer
    INSERT INTO transfers (transaction, index, transfer_t, token, value, to_addr, from_addr, rejected)
        VALUES (transaction, index, transfer_t, token_id, value, to_addr_id, from_addr_id, rejected)
        ON CONFLICT DO NOTHING;
END;
$$ LANGUAGE plpgsql;