from walletVettingModule.wallet_vetting_utils import process_wallet
from metadataAndSecurityModule.metadataUtils import get_data_from_helius
from priceDataModule.price_utils import is_win_trade

from flask import Flask, request

app = Flask(__name__)


@app.after_request
def add_cors_headers(response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    return response


@app.route("/core/analyse-wallet/<wallet_address>")
async def analyse_wallet(wallet_address):
    # might add some validation logic here
    try:
        wallet_summary = await process_wallet(wallet_address)
        return wallet_summary
    except Exception:
        return "Internal Server Error", 500


@app.route("/core/verify-token-mint/<token_mint>")
async def verify_token_mint(token_mint):
    # might add some validation logic here
    try:
        token_data = await get_data_from_helius(token_mint)
        if token_data:
            return {"result": "valid"}
        else:
            return {"result": "invalid"}
    except Exception:
        return "Internal Server Error", 500


@app.route("/core/is-win-trade")
async def api_is_win_trade():
    token = request.args.get("token")
    timestamp = request.args.get("timestamp")

    if not token or not timestamp:
        return "Missing parameter", 400
    else:
        try:
            retv = await is_win_trade(token_mint=token, timestamp=int(timestamp))
            return {"result": retv}
        except Exception as e:
            print(f"Error while checking trade {e}")
            return "Internal Server Error", 500


if __name__ == '__main__':
    app.run(debug=True)