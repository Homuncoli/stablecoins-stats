import threading

from metrics import timed
from model.Tron import addr_to_tron
from tron import address as address_module
from db_schema import copy_binary_rows

TOKEN_TABLE_LOCK = threading.Lock()
TOKEN_READY = threading.Event()

def initialize_token_cache(cur, logger):
    global TOKEN_CACHE
    if TOKEN_READY.is_set():
        return
    
    with TOKEN_TABLE_LOCK:
        if TOKEN_READY.is_set():
            return
    
        TOKEN_CACHE = TokenStash(cur, logger)
        TOKEN_READY.set()

class TokenStash:
    def __init__(self, cur, logger):
        self.by_address: dict[bytes, int] = {}
        self.by_asset_id: dict[int, int] = {}
        self._last_token_id = 0
        self._new: set[tuple[int | None, int | None, str]] = set()
        self._new_unknown_contract: set[tuple[bytes, int | None, str]] = set()
        self._state_lock = threading.Lock()

        try:
            logger.debug("Initializing token stash...")
            with timed("initialize", "stash"):
                self.refresh(cur)
            logger.debug(f"Token stash initialized with {len(self.by_address)} tokens")
        except Exception as e:
            logger.error(f"Error occurred while initializing token stash:", exc_info=e)

    def refresh(self, cur) -> int:
        with timed("refresh", "stash"):
            with self._state_lock:
                last_token_id = self._last_token_id

            cur.execute("""
                        SELECT t.id, t.asset_id, a.addr 
                        FROM tokens t LEFT JOIN addresses a 
                            ON t.contract_addr = a.id
                        WHERE t.id > %s 

                        ORDER BY id""", (last_token_id,))
            rows = cur.fetchall()

            with self._state_lock:
                for id, asset, contract_addr in rows:
                    if contract_addr:
                        self.by_address[contract_addr] = id
                    if asset:
                        self.by_asset_id[asset] = id
                    if id > self._last_token_id:
                        self._last_token_id = id

            return self._last_token_id

    def get_by_address(self, address: bytes) -> int | None:
        with self._state_lock:
            return self.by_address.get(address)

    def get_by_asset_id(self, asset_id: int) -> int | None:
        with self._state_lock:
            return self.by_asset_id.get(asset_id)

    def try_new(self, address: int | None, asset_id: int | None, token_type: str) -> bool:
        with self._state_lock:
            if (asset_id and asset_id in self.by_asset_id) or (address and address in self.by_address):
                return False
            if (address, asset_id, token_type) in self._new:
                return False
            self._new.add((address, asset_id, token_type))
        return True
    
    def try_new_unknown_contract(self, address: bytes, asset_id: int | None, token_type: str) -> bool:
        with self._state_lock:
            if (asset_id and asset_id in self.by_asset_id):
                return False
            if (address, asset_id, token_type) in self._new_unknown_contract:
                return False
            self._new_unknown_contract.add((address, asset_id, token_type))
            return True
        
    def new_snapshot(self, logger):
        with self._state_lock:
            if len(self._new) == 0 and len(self._new_unknown_contract) == 0:
                logger.debug("No new tokens to commit.")
                return [], []
            rows = [(address, asset_id, token_type) for address, asset_id, token_type in self._new]
            unresolved = [(address, asset_id, token_type) for address, asset_id, token_type in self._new_unknown_contract]
            self._new.clear()
            self._new_unknown_contract.clear()
            return rows, unresolved
    
    def commit(self, cur, staging_table, rows, unresolved, logger) -> int:
        with timed("commit", "stash"):
            for address, asset_id, token_type in unresolved:
                addr = address_module.ADDRESS_CACHE.get(address)
                if addr is None:
                    logger.warning(f"Unknown contract address {addr_to_tron(address)} for token with asset_id {asset_id} and type {token_type}")
                    continue
                rows.append((addr, asset_id, token_type))

            if len(rows) == 0:
                return 0

            cur.execute(f"TRUNCATE TABLE token_{staging_table}")

            copy_binary_rows(
                cur,
                rows,
                f"token_{staging_table}",
                "contract_addr_id, asset_id, token_type",
                ["bigint", "bigint", "text"],
            )
            
            with TOKEN_TABLE_LOCK, timed("token_insert", "db"):
                cur.execute(f"""
                    WITH staged AS (
                        SELECT DISTINCT
                            s.contract_addr_id,
                            s.asset_id,
                            s.token_type
                        FROM token_{staging_table} s
                        LEFT JOIN addresses a ON a.id = s.contract_addr_id
                        WHERE s.contract_addr_id IS NULL OR a.id IS NOT NULL
                    ),
                    inserted AS (
                        INSERT INTO tokens (contract_addr, asset_id, token_t)
                        SELECT s.contract_addr_id, s.asset_id, s.token_type::token_type
                        FROM staged s
                        ON CONFLICT DO NOTHING
                        RETURNING id, asset_id, contract_addr
                    ),
                    existing AS (
                        SELECT t.id, t.asset_id, t.contract_addr
                        FROM tokens t
                        JOIN staged s
                          ON (
                            (s.contract_addr_id IS NOT NULL AND t.contract_addr = s.contract_addr_id)
                            OR
                            (s.asset_id IS NOT NULL AND t.asset_id = s.asset_id)
                          )
                    )
                    SELECT r.id, r.asset_id, a.addr
                    FROM (
                        SELECT id, asset_id, contract_addr FROM inserted
                        UNION
                        SELECT id, asset_id, contract_addr FROM existing
                    ) r
                    LEFT JOIN addresses a ON a.id = r.contract_addr
                """)

            inserted_rows = cur.fetchall()
            inserted_count = len(inserted_rows)

            with self._state_lock:
                for token_id, asset_id, contract_addr in inserted_rows:
                    if asset_id is not None:
                        self.by_asset_id[asset_id] = token_id
                    if contract_addr is not None:
                        self.by_address[contract_addr] = token_id
                    if token_id > self._last_token_id:
                        self._last_token_id = token_id

            if inserted_count != len(rows):
                logger.warning(f"Expected to insert {len(rows)} tokens, but only {inserted_count} were inserted. This may indicate that some tokens already exist in the database.")
                self.refresh(cur)
            else:
                logger.debug(f"Inserted {inserted_count} new tokens into the database.")

            return inserted_count
        
    def size(self) -> int:
        return len(self.by_address) + len(self.by_asset_id)
                
TOKEN_CACHE: TokenStash = None