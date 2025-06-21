from decimal import Decimal
from pipeline.models import Position

def test_health_factor_basic():
    p = Position()
    p.collateral["ETH"] = Decimal("100")
    p.debt["ETH"] = Decimal("50")
    prices = {"ETH": Decimal("1")}
    lts = {"ETH": 0.8}
    assert abs(p.health_factor(prices, lts) - 1.6) < 1e-6