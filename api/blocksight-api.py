from walletVettingModule.wallet_vetting_utils import process_wallet
from metadataAndSecurityModule.metadataUtils import get_data_from_helius
from priceDataModule.price_utils import is_win_trade
from telegramModule.vet_tg_channel import vetChannel
from telegram import telegram_blueprint

from flask import Flask, request, jsonify, make_response

app = Flask(__name__)
app.register_blueprint(telegram_blueprint, url_prefix="/telegram")


@app.after_request
def add_cors_headers(response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    return response


@app.route("/core/analyse-wallet/<wallet_address>")
async def analyse_wallet(wallet_address):
    window = request.args.get("window", default=30, type=int)
    try:
        wallet_summary = await process_wallet(wallet_address, window)
        return wallet_summary
    except Exception as e:
        return f"Internal Server Error: {str(e)}", 5000


@app.route("/core/verify-token-mint/<token_mint>")
async def verify_token_mint(token_mint):
    # might add some validation logic here
    if not token_mint:
        return "bad request", 400
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

@app.route("/core/vet-tg-channel/<tg_channel>")
async def vet_channel(tg_channel):
    if not tg_channel:
        return "bad request", 400
    
    try:
        retv = await vetChannel(tg_channel)
        return {"win_rate": retv[0], "trade_count": retv[1]}
    except Exception as e:
        print(f"Error while vetting channel {tg_channel}")
        return make_response(jsonify({"status":"Internal Server Error", "message":str(e)}), 500)



if __name__ == '__main__':
    app.run(debug=True)