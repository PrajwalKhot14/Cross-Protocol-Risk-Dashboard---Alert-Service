# risk-dash/pipeline/models.py
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Dict

@dataclass
class Position:
    collateral: Dict[str, Decimal] = field(default_factory=dict)
    debt:       Dict[str, Decimal] = field(default_factory=dict)
    last_alert_ts: float = 0.0      # used later for Slack-throttling

    # Σ(collateral × LT)  /  Σ(debt)
    def health_factor(
        self,
        prices: dict[str, Decimal],
        lts: dict[str, float],
    ) -> float:
        coll_eth = sum(amt * prices[a] * Decimal(lts[a])
                       for a, amt in self.collateral.items())
        debt_eth = sum(amt * prices[a]
                       for a, amt in self.debt.items())
        return float('inf') if debt_eth == 0 else float(coll_eth / debt_eth)
    
""""
Maintains an in-memory position ledger for each wallet (collateral & debt per asset).
Calculates Health Factor (HF) in real time using Aave’s formula
HF = Σ(collateral × Liquidation Threshold) ÷ Σ(debt).

"""