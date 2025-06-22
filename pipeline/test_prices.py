from pipeline import prices

def test_caches_populate():
    WETH = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
    p = prices.price(WETH)
    lt = prices.lt(WETH)
    assert p > 0
    assert 0 < lt < 1