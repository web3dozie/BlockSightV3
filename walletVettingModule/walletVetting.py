"""
 This module provides functionality to process wallet data.
 It interfaces with pg db tables such as wallets, metadata, and token_prices
 to retrieve and update wallet information.
"""
import datetime
import random
import time
from pprint import pprint

import asyncpg

from dbs.db_operations import pg_db_url
from process_wallets_utils import wallet_processor, read_csv_wallets, remove_wallet_from_csv
from walletVettingModule.wallet_vetting_utils import process_wallet
import asyncio


async def main():
    # Process wallet data from zipped files in the specified directory
    # wallet_processor('./wallet_zips')

    wallet_list = read_csv_wallets('./wallet_counts.csv')
    print(f'There are {len(wallet_list)} wallets to process. The time is: {datetime.datetime.now().strftime("%I:%M %p")}')
    # random.shuffle(wallet_list)
    wallet_list = wallet_list[::-1]
    wallet_list = ['CF6Y1PQoFhLf3jHZSQ8BtWdLaPjp3Hzi8Z7GQQASp5Sa', '4PaCLVgyQse7STKo4qB1B7iJLk9aPsSGfdbsS8mqJ6Dj', 'GBTazpGaJwRYWZJaT9PgiRW7ga1nDpsHP397Qyn6TPRc', 'FzNXNRRCgQhVEX1Z4vKB338ACifHVFcmMJYZJnELHVkZ', 'GLf82418u3jtRDrbZeCm2p2xNrq1M38NgejuCe8XhXdh', '8aqLKcTbtErgZcFzFBk6nMkFcgocVmgNbtmNmNxv7PoW', 'CbNwYNDvzNe8UsPj6yF5WMiUT9cwGAXzsiDqUt3J3gRz', 'CSgiDhY45zXViW6FcLsUvGiok824rcJbZD3oBvWFRTuF', 'LEQr32Lemn99dLf4kaswssCZsJVuDuqQYyzKJtzP2Tm', 'DEHDQ3ANMcGANWFcybHND8S2Wp8u15S6PCAEbQvJWxDf', '9XaeRsCPUANyv98Ge1QfXDevdHD7rjpakKfYkm7Dtu9d', 'BtirhbH6hrfpQzSh8Fb5sUPEVtFufuZruuSjq3Rm6DEa', 'F4TT3XTfw3JPX2poy4ndfDcJp7rAFk9ho2Dux6fQgeRQ', '6EbMjJiz65sKV5h8tQHgFw7jkkKuR9kf5vJVpjj5kMNS', '7dX9HJ18vzni1ihU6fByzvEhwCjVfN5Dt8d1cnUZ5Tcx', '6CnhXfhqzyZBvqAnzcHRipU3cxh3bMds4VqdsCVdK9br', '2fsfTiEoUJhsu9TJkHqJCkT53yLP2b9SyUwpfFvXQFrt', 'ZoMap6zDZaZWCXKewSXXnCmxcoRaNB6rA4pMcYhsnNi', 'D7ZRfeWfWeMGgVAqs99p5x3HnhrPtgoNVej9dALf8bAc', '2x1vh8uZRGL4685opFza2dbBmWEyC7gJekk5ADqYnAnZ', '9FEHWFjgbYnFCRRHkesJNq6znHjc5Aaq7TiKi1rCVSnH', '7uihscLpHBu2iuitPUTDbi5jjcuCyCjg9V3ipzLnzL4B', 'Bjw2dbhzd5hAPZJS89gdUi3rufMGTe4kqm91gyg56bdR', 'GMrMhpXBase9So9a4R6cjps8pm8WHs2pC8MJkKBvxNcT', '37pT8jVr1zVNe2MQwtpXw8eQY7Aas6V2wBPYZiYxKknU', '8Hw9X9UwBso7Sp2CFnEEeUGW8pGDj9wghc78ccWFZWpU', '14RPS68A9YbudWfZ9n4PC4yLKNZAeDdNhmJcrAu2oS4a', 'AtmeWwb6Y6KNDgu2dPJT5dsar84bFCbNxLiW2DyYfm8p', 'CzsH4K8eBQqtAGjFkxNtP3Gcu2XY3pGw6x3y1RKFAFhG', 'Ax5f69KZZRw2dhsQhQKK7F2wrM9u5LtS4oTnpYYj5555', '871kxZi2PU4HoDB1iA1L9dyMbdJP6z4m3KUobUWnZvDn', '974a9PQxKt1zYs1ZYhXjKaaZEWmh73Q2knpvUy9kxCGw', 'CwiiPtoSZTeiPXXa2U95NUFX8kVhKAqTNwqfDkXAqgRj', 'APZw2mFLee62ZgLuhFEsHazoDRgHHo5Txpovpt8DNdYS', '7hCqzbWMsJheMpyhJ5vRv4AuAB863xdfQuUkHPMteicw', 'FLvsszvrhXwvDCbpcpowNep8h6TJSXxrVX2DbnWdCRPb', '3cdPRRsb5ZrSwMmX6GAdqtMBAkYy26nJrpBJegFV3mAN', 'Gpb3zYBGmDBEEPDiMRnerPeuC5k6XU4ri98jvrU1cm7j', '9VC7sj1i4djLuDGTtRPXDoHd66m1jATzeLKv2syJ3H31', '5jMW1hzAKZSYbLvpHf6UviQ8PoSMmdh8LY8ZYPyb94ve', 'A8GWVgVeT5Sshpd9sjE3msTF4AyC7F8Xcwnp1o4qVGME', 'Ft85SJ9ZN3MoC2oCEykkF9FoyioDg3oZuNkqqdD2ECmW', '9Nu44MGzu6VU7vMa7dc2r3ZKeoLvZVWHp9bn2u3su3vH', 'BN3igj21b6yYGnz6u1DobwyrJ9bECvw7wfutUKXTFghZ', 'Hs315CK8wtZzmSk7y71DvqqAXn7t2NTktVP6qmqbXbjs', 'Ge2FdvYxXcHbfka9hdyn6faSjvekcuMi4EeZ9JHrVYjN', 'GtGFSzwiueuCCq4HEE5ut6BAW5dr2gsTkNAzUw6b5Ea8', '9ASzBkkypyLeWqu6YthT9eFQEgufddSXWruVHnu6cxVB', 'AX3AP1ex4L8Y8iqqd72znijRcGjZg7mdo1yLwqx9SjNe', 'AQ4qnZ2MgsuvDi1oW3N85EiXkDQm48C1sAbCuk2j1foF', '3YsQqGWczeMbHBRMyS9F5FbpMexbh4WGbnyia1KDP4nK', '6vPZp1uAuX1of4uFJL1JEQRpwJVQhge6ejx2JCy2PmFs', '14iT5onJ2JfVLNeDbB5pYnsfNew3uybd3E8y3ASPSXSM', '2W6ak8jbGFxceHw2HqRbYt2catcVeedqupFkPXychgQc', '2hjRikpoVUu7KsWVPo1Tc5LezgK4eS5aukkAktBfqM8o', 'DfMewXVr3HntuP145wu6nGcwdYMYyWtk2YoZMxe7aBXq', 'DpaNQy6WLzxJjRQ1Eqmm4nDgazqgh6qkj8GMYF5Frmh', 'F7iEK5NRnRWxfAmRzUQhDPvZ25Kezrpgkwwrg8biQtQd', 'H8xCubHgwNjdoH5wXxnpGXnfe2zNJgSXwJ3KHcXkgJvn', '2cxjRs8bt5EBxbcSJkY1AXLX5UCYq23pWn83EckDMdCv', 'HHVfemxQ6zGGC5cUmYt98hWfcRuZM9w7qWXv9XaejzZa', '3h65MmPZksoKKyEpEjnWU2Yk2iYT5oZDNitGy5cTaxoE', 'E9mdA29G3bivNns6pBbiv5qXLS1CP3XiQ6eQyTycsJzX', 'BstRMYHnZLJxEjcsPfGhK3PYQephrLmmmwmKVipJn6cH', '49J648RxFF5raRNzabw3FMPzMDMKo2og8LbLwZccqLg2', '28ipXVfkdmu1PDowCHbcSfzkpH9edZmSiVoDhY5xGVfR', '5mtbmPwj2SMkxPP9c93s9oD9bmMdByTqepNarM9Y7u7e', 'AJjFk5B2P4bAaegybKJZ2PyLABkj12D8LCocAA5Kw2p2', '96R7AD9hCc3z1d4o6NBZZXvZgpoDoac6NbucA99JSn37', 'D68Jqb7jEZAGtav4aVui536mtT1KAJ4bKwYgFqy8etW7', '7cWXArAEQQPHahEvYs1qHrs7kfNMGjcJQpwk8mDKtFyC', '45yBcpnzFTqLYQJtjxsa1DdZkgrTYponCg6yLQ6LQPu6', '7GXFMLn7ib4VccZ6u6y3Xzg6C8ruAqZK6WTwdqLj1o8K', '78WJuvGZ8sHvdrHBXM79HehqUUn5FmvNmAwpkwiJiRGP', '9hy2BKD9yf5H6HWR4Vr8xnsNdvnYrZEjqx3H7vp4gbGe', '25Kp7ipf7BzRbyKGahozYzX8GF3CpGygrC7etUQy3jR5', '2PDxEVKMa1qPG9rUs2MgGxQ7hM74shfau2TBbeg6iZqf', '2Jew9U6LWYeFJyjHr3yxsq2JvnkpzN9Lu1u3485UZuCb', '7F1UmhjwGeRoDLuoZN6nrvYQjgiUAufvKoGEkH6eFeLx', '76CWEDyQRLKdwCibXq7sv6AzpbrBzAvnJroUxmJQtSaW', 'BpGZBEotGg5EqbhmGnrYCdZJqfjKXwyCERVCcDwDxnSZ', '5B1jWQKDisQhd2H4PcZG7f4DTYijHEWyzhMJC6sAZVNt', '6A7gNYsfjQU7gEqhvCFsLxZD9XspdxjqdtBNyaKCSBk2', 'Ba4j5B6PjZjHufZz5Ghmh84VLZJ2EamoGmE1drLHs54o', '83dxcf3tAKiGYHLwHoByLqerazdCr5pkFK97LbwyRFWg', '2ZbyFigBhSKVt7Byk4n7wz8pa184cNwWtNvCjfXzi6wj', 'CSKc6y6RSzhiKPd65PTLncHiWjF8Jw5rt1eTZF3SwSbD', '7Yeq8skEPcBYpCGAq8xgh5PoggCR9i6TtBtTtyBLCEv', 'GQ26EXd1K4cbSiPaCxDTLNmt63Ef6AUFNo8aXNe1KhBf', '2WUCXeXS7dqkqyr4GVWRBBPyTdzsSbLsFebPBWsJ9H5h', 'BrmvfRBVqgMeAjozL7FFvMwZMvf6BuBG1m5ZUGAgdN9Y', 'ApfcioX1w5rfn8Ube46ZSAW1Sad5iEVs2Pv47iMKguTd', 'AuLNLSmnXHRqaCTJjpVNtFnMN397VRgXMVttLGT7mhVT', '4TUL54TRpQiafyndwf4M8m7uuRZ4if9vjhSfVBGu71XY', '4SXyeQqnkE6GK1APW1acdkMZ6sPbAocD2BgDsLyWvwJk', '5YkZmuaLhrPjFv4vtYE2mcR6J4JEXG1EARGh8YYFo8s4', 'BeYx4DZ9n8NqNzLxcw9XcKDPT1pCNk8pM5M1LmDUzSvt', 'DTvmbs79auofg48TEcw16FWmUXK4f3kHHS6Kk4crP9uc', '7hghdRVqZ29VBuVXrR7i1ptgdZEYtAFCapyLBb2h181c', '5eaXyHVu1ZRjxEoZZxUA55M8aXFhKUrpurycTitSSebY', 'oU7ZbNwiV7UbwCz9LCCLBjsJd13wJdkTufq8TWWboDX', '8baFtXBfiqBisijYrcNDY4fU9UpjTyZJM5J5ATTgHbJx', '6nAn32uERpMP5ZMk1Hu4WdQPrxPGb5Cgp54vLvdroi12', 'sCMNrLxCr3k6mYrvQ4FTZ3bZm9f7S1hmkrBUHqKcKiA', '7XURCfraWRfNoNBk9wNqFmoWUdCGrDZXrGoWSc7WE5Nv', 'EgiPGB9bqnf157TchaUBrnZU5Zp6FZV2jnfQb8xzTVse', '2m5498hY3hpSvm1pVyZwwyU6bpQGFGu3PADSoEoZFXQB', '4MB72B3BBCxuEqWPBDCghaET5CdfPc6R2ZD5bEaeyfD1', 'CoS7AcV6cHsD8ReZysJ86tNvF462TNhGdZV4VkNqM3Ne', '3Gc1a7RGCbm2hZupTyQxxQrrMzYT87qvTkK3DWskqhha', '8a7KUy2EK2EVEwBpHT1zjRryxpJAsi4HjHk7sgKu6etH', 'D1KBfZWPFvVAD1qxj4FdbzTXmiY8PGFumKTqMx5jAx5Q', 'RFSqPtn1JfavGiUD4HJsZyYXvZsycxf31hnYfbyG6iB', 'ET1ySKbb4kFpVJXcAh13YEJXS6CtUmRN851JU843YAHD', 'DcmSREqbZFyFL8ApVYH7daua1R77qohdC6uHotcsA3Hr', '3jN1M8gWLk2ryTGnscrcwRK1Gy4Ttzq5QizWT8uizZsT', 'ALtJR68PpForuyeH9dn27NUxZk76cvfcxVCWuMtWuVdY', 'GVg53PS9FA5Y7gvvuSPVJzfxxCQSHTpChKNb5Tp4m8JW', 'B719mMqUd5xFoo6zXxAaXRgk1Qg6wvNaCgGmf3GNWNVG', '4dEVRNjVK6XZQiTNgoCBgZEm24mJhVMvco82x3TRfmmm', 'FUbnme4UJ3x6aZ5nGPymTWoDghE9p3XJx1BfLzSpGefM', 'A1syLWNJRxzswsLbphsXbW74gJpg1ARAuyLTTZHTmUpj', 'EYiv4TGvmk8DZvwE5iQFrTxBNXfePAcorNYA7Qaf2TN9', 'FmrU3Hq3Q4W8bhEGvNr92WY8KWhPmMiUms2hnLF6u8mA', 'HNFRDptF6XeRLdig7fQpvbJqnPX6REM6hmrStW8XdkdZ', '9HJTeX467VZNKn5nv8VjXAxtwnbKmi8LfjUSieVDbNxt', 'JED9gEbMzhAsKHt65DjrDJ3epWCnoDiZPyn3YhQkqC38', '58aJk9ngALL8Np7r51JWPc3buPthaPDKknmDaNxuGQcP', 'D6GYaAdUp4EbfMK7vU6XHmKaS5WhtohVa5rCyh2ZX59v', '6dsXfXb7NojREt1RHJN6wFXLD7VTz2NBidy7o9yogM5N', 'AmPsF7JyfbGo2HVafezdphAq56ZD9DdidriYgeLJGDx2', '2bEukck7JUGgGYrh8anHJ2NAZXjTNeSSVVrL1dWNMP29', '7UxpzHbeyhXVXvF52X7gNpP6yJ1sLQuhGTin4TNi1PhT', '2yGU9ApnB9xTPGynEU7dugEvR3hqVxsGnzbssrX4Nj9G', 'JATPD7rMktj3vDaiipY1Yg6vVbUDbrnvRctAvazRD6Q7', 'EHsuLH18EGHmjmSjg8DbZXXhJNNoVTwVHFiomPN2jouu', '5jA4okTrdjocnMuoD288zxSp5V9J86JG7hwSkQ9T1CkQ', '4sUGZ1fBML3xcAX31qkNiDe8zT5ywCFtKLmLt2jfJ88b', 'FLtrBCVaHFD28vrS6ajeUuCy7iaBDjKiqGq5EcgqcvRb', 'N4Sjvmb7zbhuyqWczB9XkXe4D5q3cS48PaGUQyxoSxP', '6xgTUrJ4ty79RWz6rbMHAVcFZDYF8kX5o1iCjXds6uCW', '7Cbozw5Un2cLd2rvCtZuwWrQ3oSoFnHWMEywSU27JJkc', '3CL2Jp2Wjq7DKA6Uyte5iY7BWgzrSqnzFdH7GERuavbX', '7xUMWUrgfiEmvohurE59my6y7YykwcDiLf2KerwXRjJo', '51hydZ9jiB8bCU6xRhTL6UvV11eawvfrCoCr9d17wbtE', 'CGzFWR3yQG5mz35d5uUPbaBzC3AL7H57rsDi4G862FmH', '8Ei1NADY8dgVGxwZgEdc8YvVFkd3cd6JbcXYnyUqrp8A', '9GNZhQtAuRim6z6gUoaAeKtjXvPZHogv663vCdX1h9YY', '4w7petYTMikTbjqjHNsE1C97ormD4oyPonbe5f4wkxKz', '5wTebj56mue3YBpLK9SbCMe5MziqEvPFGuMz9SbgnuEw', '8y6KvdjYnyXM9cWTrjAkEdj8hmcAddkfJ9nKiYRQLwf7', '7uKVbVDJojkY9MAJmWQqzp3Hpm7tPikgNiZr2UvYxqRg', '6516KNf8XD1pfwCzfebmBb6d9RRXgMeyeDgJiKhb7tbe', '6xUL8CUfV1fzd3UQoDBs7agWNXpwyE5q56css1wHNFFU', '8tR7ooZxBQJx4JijAt3eD6wo4rETDiE7S4eMzMbzYMJ7', '9Ks1JcSpoPrKHYd9Yq2yaVg3AxU2Y5XvoD8jPVmAsXcT', '9fBnqyiERXd5v8vgph2xQa8KsNk9J4mU3vLqF4Lw3G5a', '4fELUqyVjk3w3eQqVYWFxRd1tUJeuh9XtDRU64LUyp2N', '4xUkaEGkKDPVETNv5Asd4D7195A8ZGWQjWRHoSy6bd1r', '7utFnhb3BRqgPXjemsusT4GijbDTdEPydQeoHwbDgdu4', '3gcTZSNgGDzM8m9U1EGZ6eEykmJEWtpMxMQE7y5Mpj33', '5EvXACvDLCxDxV2iG6nSNC85KZKR8GkUQRKZe2va88zP', 'EbnR8nwcMesfqNv4XPt4eHAxanydd3gxEyM8WHthFVnY', '8vGJcE7Bk5UicMnwkNh9ChACkbAugC8hzYGDuo9VY1Uu', 'BMAqbTivdtxr4awsxLS9xiTGWdqzon5jxY1jRaqkpUQQ', 'DNXpTE8VnQkoLuBj6TgPS8W2TYDGFQUYu6c2w49YRLaf', 'DKqK6mwErKzkL4Fxnb5fk5iLMFE7qru4PBTW9xYENFzX', '62D7BoBe6F1sqZ9CfxGaFMkx3k1qhZKGueCuAxVDD9pd', 'A3mja9eFSFhSXaiux88Tp5jY4fZnSbkm1uFTWyuG8e1D', '63gM4mVKnJDTDEZEv6q8iMBrizQSUVsDmNSnqmQXDiZY', 'HKtzTLqANw5JjCSipquBSg5j1bUBdobAYfbBJtfvjpZW', '2NgBnvWrtMQ2vVd2sz8qdthUKor8BKFr49coXuJiy4vF', 'A6aay4yh8dv5cgkC6JwjnEmFpWE7SmaTVKgXZXUWaUWB', 'EHB3CjFbLMr5ghbzsSEMwU93tadhQrdykmC8wJuB9dek']


    semaphore = asyncio.Semaphore(10)
    pool = None
    try:
        # Try to initialize the pool
        try:
            pool = await asyncpg.create_pool(dsn=pg_db_url, min_size=300, max_size=800, max_inactive_connection_lifetime=1000, command_timeout=500)
        except Exception as e:
            print(f"Failed to create pool: {e}")
            return

        async def process_wallet_with_semaphore(wallet):
            async with semaphore:
                start = time.time()
                try:
                    retv = await process_wallet(wallet, pool=pool)
                except Exception as e:
                    print(f"Error processing wallet {wallet}: {e}")
                    return
                end = time.time()
                pprint(f"Processed {wallet} in {end-start:.2f} seconds")
                if retv is not None:
                    remove_wallet_from_csv('./wallet_counts.csv', wallet)

        # wallet_list = list(reversed(wallet_list))
        tasks = [process_wallet_with_semaphore(wallet) for wallet in wallet_list]
        await asyncio.gather(*tasks)

    finally:
        if pool:
            await pool.close()

if __name__ == '__main__':
    asyncio.run(main())
