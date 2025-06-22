# pipeline/prices.py
import json, time, os
from decimal import Decimal
from pathlib import Path
from dotenv import load_dotenv
from web3 import Web3

load_dotenv("env/.env")

# ------------------------------------------------------------------ contracts
AAVE_ORACLE       = "0x54586bE62E3c3580375aE3723C145253060Ca0C2"
UI_POOL_DP        = "0x3F78BBD206e4D3c504Eb854232EdA7e47E9Fd8FC"
POOL_ADDRESSES_PROVIDER  = "0x2f39d218133AFaB8F2B819B1066c7E434Ad94E9e"

def _load_abi(name: str):
    text = Path(f"ingest/abi/{name}").read_text()
    data = json.loads(text)
    # if it’s the full Hardhat/Truffle artifact, grab the "abi" field
    if isinstance(data, dict) and "abi" in data:
        return data["abi"]
    # if it’s already just an ABI list (Etherscan-download), return it directly
    if isinstance(data, list):
        return data
    raise ValueError(f"Unrecognized ABI format in {name}")

w3      = Web3(Web3.HTTPProvider(os.environ["ALCHEMY_HTTP_URL"]))
oracle  = w3.eth.contract(address=AAVE_ORACLE, abi=_load_abi("AaveOracle.json"))
dp      = w3.eth.contract(address=UI_POOL_DP,  abi=_load_abi("UiPoolDataProviderV3.json"))

# ------------------------------------------------------------------ caches
_price: dict[str, Decimal] = {}
_lt:    dict[str, float]   = {}
_last   = 0
REFRESH = 15          # seconds

def _refresh():
    global _last
    if time.time() - _last < REFRESH:
        return

# 1) grab both the per-reserve data *and* the market’s base-currency info
reserves_data, base_currency = dp.functions.getReservesData(POOL_ADDRESSES_PROVIDER).call()
mrc_unit = base_currency[0]

# 2) extract priceInMarketReferenceCurrency (field #22) & threshold (field #5)
for entry in reserves_data:
    asset = entry[0] 
    # price in ref-currency *with* mrc_unit decimals → normalize
    raw_mrc_price = entry[22]
    _price[asset] = Decimal(raw_mrc_price) / Decimal(mrc_unit)

    # reserveLiquidationThreshold (bps)
    bps = entry[5]
    _lt[asset] = bps / 10_000



    _last = time.time()

# ------------------------------ public helpers ------------------------------
def price(asset: str) -> Decimal:
    _refresh()
    return _price[asset]

def lt(asset: str) -> float:
    _refresh()
    return _lt[asset]

def price_cache(): return _price.copy()
def lts_cache():   return _lt.copy()
