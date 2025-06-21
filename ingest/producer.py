import os, json, asyncio
from dotenv import load_dotenv
from web3 import AsyncWeb3, WebSocketProvider
from web3.utils.subscriptions import LogsSubscription
from kafka import KafkaProducer
from web3.exceptions import MismatchedABI
load_dotenv("env/.env")

async def main():
    # 1) connect via WebSocket

    w3 = AsyncWeb3(WebSocketProvider(os.getenv("ALCHEMY_WS_URL")))
    await w3.provider.connect()

    # load ABI & contract
    with open("ingest/aave-firehose/Pool.json", encoding="utf-8-sig") as f:
        abi = json.load(f)["abi"]
    pool = w3.eth.contract(
        address="0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2",
        abi=abi
    )

    # 2) Kafka producer (and connectivity check)
    producer = KafkaProducer(
        bootstrap_servers=os.getenv("KAFKA_BROKERS").split(","),
        value_serializer=lambda v: json.dumps(v).encode(),
        key_serializer=lambda k: k.encode(),
        linger_ms=50,
    )
    if not producer.bootstrap_connected():
        raise RuntimeError("⛔️ Cannot connect to Kafka brokers")

    # 3) Build a topic → event‐instance map in the outer scope
    supply_evt = pool.events.Supply()
    borrow_evt = pool.events.Borrow()
    repay_evt  = pool.events.Repay()

    event_map = {
        supply_evt.topic: supply_evt,
        borrow_evt.topic: borrow_evt,
        repay_evt.topic:  repay_evt,
    }

    # 4) Handler just closes over that map
    async def handle_event(ctx):
        log = ctx.result
        print(f"📥  got log: topics={log['topics']}")

        ev = None
        for EventFactory in (pool.events.Supply, pool.events.Borrow, pool.events.Repay):
            try:
                # instantiate the event and try to decode
                ev = EventFactory().process_log(log)
                # if no exception, we’ve found the right ABI
                break
            except MismatchedABI:
                # wrong signature for this ABI—try the next one
                continue

        if not ev:
            print("🔍  skipping—unrecognized event")
            return

        # success!
        print("✅  decoded event:", {
            "event":   ev.event,
            "tx_hash": ev.transactionHash.hex(),
            "user":    ev.args.get("user", ev.args.get("onBehalfOf")),
            "asset":   ev.args["reserve"],
            "amount":  str(ev.args.get("amount", 0)),
            "block":   ev.blockNumber,
        })

        payload = {
            "event":   ev.event,
            "tx_hash": ev.transactionHash.hex(),
            "user":    ev.args.get("user", ev.args.get("onBehalfOf")),
            "asset":   ev.args["reserve"],
            "amount":  str(ev.args.get("amount", 0)),
            "block":   ev.blockNumber,
            "ts":      (await w3.eth.get_block(ev.blockNumber))["timestamp"],
        }
        print("✅ decoded:", payload)

         # Add event-specific fields
        if ev.event == "Borrow":
            payload.update({
                "interestRateMode": ev.args["interestRateMode"],
                "borrowRate":       str(ev.args["borrowRate"]),
                "referralCode":     ev.args["referralCode"],
            })
        elif ev.event == "Repay":
            payload.update({
                "repayer":   ev.args["repayer"],
                "useATokens": ev.args["useATokens"],
            })

        # Log and send
        print("✅  decoded event:", payload)
        producer.send("aave-raw", key=payload["user"], value=payload) \
                .add_callback(lambda md: print(f"✔ sent {md.topic}@{md.partition}/{md.offset}")) \
                .add_errback(lambda e: print("❌ Kafka error:", e))

    # 5) Single LogsSubscription for exactly those three topics
    subscription = LogsSubscription(
        label="Aave Pool S/B/R",
        address=pool.address,
        topics=[[ *event_map.keys() ]],
        handler=handle_event
    )
    await w3.subscription_manager.subscribe([subscription])
    print("⛽ Subscribed – streaming only Supply/Borrow/Repay…")

    # 6) Pump forever
    try:
        await w3.subscription_manager.handle_subscriptions()
    finally:
        print("🛑 Shutting down…")
        producer.flush(timeout=10)
        producer.close(timeout=10)

if __name__=="__main__":
    asyncio.run(main())
