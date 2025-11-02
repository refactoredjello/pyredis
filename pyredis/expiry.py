import asyncio
import math
import traceback

from pyredis.store import DataStoreWithLock

SAMPLE_SIZE = float(".2")
INTERVAL_SECONDS = 300


async def run_cleanup_in_background(
    datastore: DataStoreWithLock, interval_seconds=INTERVAL_SECONDS
):
    """Get 20% of the random keys, if the key expired, datastore will cull it automatically"""
    print(f"Expiry Interval: {interval_seconds} seconds")
    while True:
        try:
            size = datastore.size()
            if size:
                count = math.ceil(size * SAMPLE_SIZE)
                for _ in range(count):
                    key = datastore.get_random_key()
                    datastore.get(key)

        except asyncio.CancelledError:
            print("Cleanup task cancelled")
            break
        except Exception:
            print("Expiry scheduler encountered an issue")
            traceback.print_exc()
        await asyncio.sleep(interval_seconds)
