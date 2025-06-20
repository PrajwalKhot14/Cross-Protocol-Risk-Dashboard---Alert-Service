import os
import json
import asyncio
from dotenv import load_dotenv
from web3 import AsyncWeb3, WebSocketProvider
from kafka import KafkaProducer

load_dotenv("env/.env")  # ① load ALCHEMY_WS_URL, KAFKA_BROKERS


async def main():
    # 1) connect via WebSocket
    w3 = AsyncWeb3(WebSocketProvider(os.getenv("ALCHEMY_WS_URL")))
    await w3.provider.connect()
    # --- use utf-8-sig to drop the BOM ---
    # load the Hardhat artifact...
    with open("aave-firehose/Pool.json", encoding="utf-8-sig") as f:
        abi = json.load(f)["abi"]
    pool = w3.eth.contract(
        address="0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2",
        abi=abi
    )

    # # 2) Check the chain ID
    # try:
    #     chain_id = await w3.eth.chain_id
    # except Exception:
    #     # fallback if chain_id isn’t available
    #     chain_id = await w3.eth.get_chain_id()
    # print("🌐  Connected chain ID:", chain_id)

    # # 3) Get the latest block number
    # latest = await w3.eth.get_block("latest")
    # print("⛓️  Latest block number:", latest["number"])

    # # 4) Verify there is bytecode at the Pool address
    # code = await w3.eth.get_code("0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2")
    # print("📦  Pool code exists?:", bool(code), f"(length={len(code)})")

    # 2) set up Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=os.getenv("KAFKA_BROKERS").split(","),
        value_serializer=lambda v: json.dumps(v).encode(),
        key_serializer=lambda k: k.encode(),
        linger_ms=50,
    )

    # 3) compute the topic‐filters for Supply / Borrow / Repay
    supply_topic = pool.events.Supply().topic
    borrow_topic = pool.events.Borrow().topic
    repay_topic  = pool.events.Repay().topic

    filter_params = {
        "address": pool.address,
        "topics": [[supply_topic, borrow_topic, repay_topic]]
    }

    # 4) handler for any incoming log
    async def handle_event(handler_context):
        log = handler_context.result
        print("⚡  raw log received:", log)  # <— DEBUG

        # decode into the correct event type
        ev = None
        for name in ("Supply", "Borrow", "Repay"):
            try:
                ev = getattr(pool.events, name)().processLog(log)
            except Exception:
                continue
            else:
                break

        if not ev:
            print("🔍  skipped unrecognized event")  # <— DEBUG
            return  # skip unknown logs

        # build and send payload
        payload = {
            "event":   ev.event,
            "tx_hash": ev.transactionHash.hex(),
            "user":    ev.args.get("user", ev.args.get("onBehalfOf")),
            "asset":   ev.args["reserve"],
            "amount":  str(ev.args.get("amount", ev.args.get("amountScaled", 0))),
            "block":   ev.blockNumber,
            "ts":      (await w3.eth.get_block(ev.blockNumber))["timestamp"],
        }
        print("✅  decoded payload:", payload)       # <— DEBUG
        producer.send("aave-raw", key=payload["user"], value=payload)

    # 5) subscribe via eth_subscribe("logs", …) with our handler
    sub_id = await w3.eth.subscribe(
        "logs", 
        filter_params, 
        handler=handle_event
    )
    print(f"⛽  Subscribed (ID={sub_id}) – streaming only Supply/Borrow/Repay…")

    # 6) pump the socket until you cancel
    try:
        # this runs until cancelled by KeyboardInterrupt
        await w3.subscription_manager.handle_subscriptions(run_forever=True)
    finally:
        # guaranteed cleanup on exit
        print("🛑  Shutting down, flushing Kafka producer…")
        producer.flush(timeout=10)
        producer.close(timeout=10)
        print("✅  Kafka producer closed")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("👋  Interrupted by user, exiting")
