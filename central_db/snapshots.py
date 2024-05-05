async def take_snapshot(mint='', pool=None):
    conn = await pool.acquire()