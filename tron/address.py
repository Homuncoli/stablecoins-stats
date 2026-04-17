from contextlib import nullcontext
import threading

from metrics import timed
from db_schema import copy_binary_rows

ADDR_TABLE_LOCK = threading.Lock()
ADDR_READY = threading.Event()

def initialize_address_cache(cur, logger):
    global ADDRESS_CACHE
    if ADDR_READY.is_set():
        return
    
    with ADDR_TABLE_LOCK:
        if ADDR_READY.is_set():
            return
    
        ADDRESS_CACHE = AddressStash(cur, logger)
        ADDR_READY.set()

class AddressStash:
    def __init__(self, cur, logger):
        self._cache: dict[bytes, int] = {}
        self._last_address_id = 0
        self._new: dict[bytes, str] = {}
        self._state_lock = threading.Lock()

        try:
            logger.debug("Initializing address stash...")
            with timed("initialize", "stash"):
                self.refresh(cur)
            logger.debug(f"Address stash initialized with {len(self._cache)} addresses.")
        except Exception as e:
            logger.error(f"Error occurred while initializing address stash:", exc_info=e)

    def refresh(self, cur) -> int:
        with timed("refresh", "stash"):
            with self._state_lock:
                last_address_id = self._last_address_id

            cur.execute("SELECT id, addr FROM addresses WHERE id > %s ORDER BY id", (last_address_id,))
            rows = cur.fetchall()

            with self._state_lock:
                for address_id, address in rows:
                    self._cache[address] = address_id
                    if address_id > self._last_address_id:
                        self._last_address_id = address_id
                return self._last_address_id

    def get(self, address: bytes) -> int | None:
        # Read path is intentionally lock-free for throughput.
        return self._cache.get(address)

    def try_new(self, address: bytes, addr_type: str) -> bool:
        if not isinstance(address, bytes):
            print(f"Trying to add address: {address} with type {type(address)} of {addr_type}")
            raise ValueError("String addresses are not supported in try_new. Please provide bytes-like input.")
        with self._state_lock:
            if address in self._cache:
                return False
            if address in self._new:
                return False
            self._new[address] = addr_type
            return True
    
    def new_snapshot(self, logger):
        with self._state_lock:
            if len(self._new) == 0:
                logger.debug("No new addresses to commit.")
                return []
            rows = list(self._new.items())
            self._new.clear()
            return rows
    
    def commit(self, cur, staging_table, rows, logger) -> int:
        with timed("commit", "stash"):
            cur.execute(f"TRUNCATE TABLE addr_{staging_table}")

            copy_binary_rows(
                cur,
                rows,
                f"addr_{staging_table}",
                "addr, addr_t",
                ["bytea", "text"],
            )

            with ADDR_TABLE_LOCK:
                cur.execute(f"""
                    INSERT INTO addresses (addr, addr_t)
                    SELECT addr, addr_t::addr_type
                    FROM addr_{staging_table}
                    ON CONFLICT (addr) DO UPDATE
                    SET addr_t = addresses.addr_t
                    RETURNING id, addr;
                """)

                inserted_rows = cur.fetchall()

            inserted_count = len(inserted_rows)
            
            with self._state_lock:
                for address_id, address in inserted_rows:
                    if address_id is None:
                        logger.warning(f"Failed to insert address {address.hex()}, skipping.")
                        continue
                    self._cache[address] = address_id
                    if address_id > self._last_address_id:
                        self._last_address_id = address_id
            
            if inserted_count != len(rows):
                logger.warning(f"Expected to insert {len(rows)} addresses, but only {inserted_count} were inserted. This may indicate that some addresses already exist in the database.")
                self.refresh(cur)
            else:
                logger.debug(f"Inserted {inserted_count} new addresses into the database.")
            return inserted_count
        
    def size(self) -> int:
        return len(self._cache)

ADDRESS_CACHE: AddressStash = None