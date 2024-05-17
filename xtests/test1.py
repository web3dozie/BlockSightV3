async def process_wallet(wallet_address: str, window: int = 30, pool=None) -> dict:
    """
    Process a wallet by fetching transactions, calculating PnL, and updating the database.

    Args:
        pool:
        wallet_address (str): The wallet address to process.
        window (int): The number of days to consider for transaction history.

    Returns:
        dict: A dictionary containing the processed wallet summary.
    """

    if await is_wallet_outdated(wallet_address, window=window, pool=pool):
        pass


    else:
        return await get_wallet_data(wallet_address, pool=pool)

    if window == 30:
        await process_wallet(wallet_address, window=7, pool=pool)
    if window == 7:
        await process_wallet(wallet_address, window=3, pool=pool)