from __future__ import annotations

from typing import Literal

from pydantic import BaseModel


class TradeCallbackData(BaseModel):
    action: Literal["yes", "no"]
    pair: str
    exch_sell: str
    exch_buy: str
    spread: float

    @classmethod
    def from_str(cls, data: str) -> TradeCallbackData:
        parts = data.split("|")
        if len(parts) != 5:
            raise ValueError(f"expected 5 fields, got {len(parts)}: {data!r}")

        return cls(
            action=parts[0],
            pair=parts[1],
            exch_sell=parts[2],
            exch_buy=parts[3],
            spread=float(parts[4]),
        )

    def to_str(self) -> str:
        return f"{self.action}|{self.pair}|{self.exch_sell}|{self.exch_buy}|{self.spread}"
