import os, json, asyncio, logging
from dotenv import load_dotenv
from web3 import AsyncWeb3, WebSocketProvider
from web3.utils.subscriptions import LogsSubscription
from kafka import KafkaProducer
from web3.exceptions import MismatchedABI, Web3Exception
from websockets.exceptions import ConnectionClosedError
load_dotenv("env/.env")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

ALCHEMY_WS_URL = os.getenv("ALCHEMY_WS_URL")
KAFKA_BROKERS  = os.getenv("KAFKA_BROKERS", "localhost:9092").split(",")

POOL_ADDRESS = "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2"
ABI_PATH     = "ingest/abi/Pool.json"
TOPIC        = "aave-raw"

async def main():
    with open(ABI_PATH, encoding="utf-8-sig") as f:
        pool_abi = json.load(f)["abi"]

    # Kafka producer (keep across reconnects)
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKERS,
        value_serializer=lambda v: json.dumps(v).encode(),
        key_serializer=lambda k: k.encode(),
        linger_ms=50,
    )
    if not producer.bootstrap_connected():
        raise RuntimeError("⛔️ Cannot connect to Kafka brokers")
    
    while True:
        try:
            # 1) Connect Web3 via WS
            w3 = AsyncWeb3(WebSocketProvider(ALCHEMY_WS_URL))
            await w3.provider.connect()
            logging.info("🌐 Connected to Ethereum WS")

            # 2) Instantiate contract and event factories
            pool = w3.eth.contract(address=POOL_ADDRESS, abi=pool_abi)
            supply_evt = pool.events.Supply()
            borrow_evt = pool.events.Borrow()
            repay_evt  = pool.events.Repay()
            liquidate_evt   = pool.events.LiquidationCall()
            event_factories = (supply_evt, borrow_evt, repay_evt, liquidate_evt)

            # 3) Event handler
            async def handle_event(ctx):
                log = ctx.result
                # try decode with each factory
                ev = None
                for factory in event_factories:
                    try:
                        ev = factory.process_log(log)
                        break
                    except MismatchedABI:
                        continue
                if ev is None:
                    return

                payload = {
                    "event":   ev.event,
                    "tx_hash": ev.transactionHash.hex(),
                    "block":   ev.blockNumber,
                    "ts":      (await w3.eth.get_block(ev.blockNumber))["timestamp"],
                }
                # add extra fields
                if ev.event in ("Supply", "Borrow", "Repay"):
                    payload.update({
                        "user":   ev.args.get("user", ev.args.get("onBehalfOf")),
                        "asset":  ev.args["reserve"],
                        "amount": str(ev.args.get("amount", ev.args.get("amountScaled", 0))),
                    })

                    if ev.event == "Borrow":
                        payload.update({
                            "interestRateMode": ev.args["interestRateMode"],
                            "borrowRate":       str(ev.args["borrowRate"]),
                            "referralCode":     ev.args["referralCode"],
                        })
                    elif ev.event == "Repay":
                        payload.update({
                            "repayer":    ev.args["repayer"],
                            "useATokens": ev.args["useATokens"],
                        })
                    out_topic = TOPIC

                elif ev.event == "LiquidationCall":
                    print("🔍 args keys:", list(ev.args.keys()))
                    payload.update({
                        "liquidated_user":        ev.args["user"],
                        "liquidator":             ev.args["liquidator"],
                        "collateral_asset":       ev.args["collateralAsset"],
                        "debt_asset":             ev.args["liquidationAsset"],
                        "debt_to_cover":          str(ev.args["debtToCover"]),
                        "collateral_amount":      str(ev.args["liquidatedCollateralAmount"]),
                        "receive_a_token":        ev.args["receiveAToken"],
                    })
                    out_topic = "aave-liquidations"
                else:
                    out_topic = TOPIC

                # send to Kafka
                producer.send(out_topic, key=payload["user"], value=payload) \
                        .add_callback(lambda md: logging.info(f"Sent {md.topic}@{md.partition}/{md.offset}")) \
                        .add_errback(lambda e: logging.error("Kafka error:", e))

            # 4) Subscribe
            subscription = LogsSubscription(
                label="Aave Pool S/B/R/L",
                address=POOL_ADDRESS,
                topics=[[supply_evt.topic, borrow_evt.topic, repay_evt.topic, liquidate_evt.topic]],
                handler=handle_event
            )
            await w3.subscription_manager.subscribe([subscription])
            logging.info("⛽ Subscribed – streaming Supply/Borrow/Repay…")

            # 5) Pump until error
            await w3.subscription_manager.handle_subscriptions()

        except (ConnectionClosedError, asyncio.IncompleteReadError, Web3Exception) as e:
            logging.warning(f"⚠️  WebSocket dropped: {e!r}. Reconnecting in 5s…")
            producer.flush(timeout=10)
            producer.close(timeout=10)
            await asyncio.sleep(5)
            continue

        except KeyboardInterrupt:
            logging.info("👋 Interrupted by user, shutting down…")
            break

        finally:
            # clean up producer on exit or before retry
            try:
                producer.flush(timeout=10)
                producer.close(timeout=10)
            except Exception:
                pass

    logging.info("Producer terminated.")
    producer.flush(timeout=10)
    producer.close(timeout=10)

if __name__=="__main__":
    asyncio.run(main())
