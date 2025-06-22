"""
Save UiPoolDataProviderV3 ABI to ingest/abi/.

Needs:
  pip install requests python-dotenv
  env/.env  ->  ETHERSCAN_API_KEY=your_key_here
"""

import os, json, sys
from pathlib import Path
import requests
from dotenv import load_dotenv

load_dotenv("env/.env")

ADDRESS  = "0x3F78BBD206e4D3c504Eb854232EdA7e47E9Fd8FC"      # UiPoolDataProviderV3
API_KEY  = "ETHERSCAN_API_KEY"
OUT      = Path("ingest/abi/UiPoolDataProviderV3.json")
URL      = (
    "https://api.etherscan.io/api"
    "?module=contract&action=getabi"
    f"&address={ADDRESS}&apikey={API_KEY}"
)

if not API_KEY:
    sys.exit("Set ETHERSCAN_API_KEY in env/.env")

resp = requests.get(URL, timeout=15).json()
if resp["status"] != "1":
    sys.exit(f"Etherscan error: {resp.get('message')} – {resp.get('result')}")

abi = json.loads(resp["result"])
OUT.parent.mkdir(parents=True, exist_ok=True)
OUT.write_text(json.dumps(abi, indent=2))
print(f"✅  ABI saved to {OUT}")
