import time

from metadataAndSecurityModule.metadataUtils import get_full_dxs_data


async def take_snapshot(mint='', pool=None):
    conn = await pool.acquire()

    dxs_data = await get_full_dxs_data()
    timestamp = int(time.time())
