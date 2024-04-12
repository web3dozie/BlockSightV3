from walletVettingModule.wallet_vetting_utils import process_wallet
from flask import Flask

app = Flask(__name__)
@app.after_request
def add_cors_headers(response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    return response


@app.route("/core/analyse-wallet/<wallet_address>")
async def analyse_wallet(wallet_address):
  # might add some validation logic here
  return await process_wallet(wallet_address)