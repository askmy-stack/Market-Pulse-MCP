"""Rolling feature calculations."""

from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass, field


@dataclass
class RollingWindow:
    window_size: int = 20
    prices: deque[float] = field(default_factory=deque)
    volumes: deque[int] = field(default_factory=deque)

    def add(self, price: float, volume: int) -> None:
        self.prices.append(price)
        self.volumes.append(volume)
        while len(self.prices) > self.window_size:
            self.prices.popleft()
            self.volumes.popleft()

    @property
    def ready(self) -> bool:
        return len(self.prices) >= 2

    def compute_returns(self) -> tuple[float, float]:
        if len(self.prices) < 2:
            return 0.0, 0.0
        prices = list(self.prices)
        ret_1m = (prices[-1] - prices[-2]) / prices[-2] if prices[-2] else 0.0
        lookback = min(5, len(prices) - 1)
        ret_5m = (
            (prices[-1] - prices[-1 - lookback]) / prices[-1 - lookback]
            if prices[-1 - lookback]
            else 0.0
        )
        return ret_1m, ret_5m

    def compute_volatility(self) -> float:
        if len(self.prices) < 3:
            return 0.0
        prices = list(self.prices)
        returns = [
            (prices[i] - prices[i - 1]) / prices[i - 1]
            for i in range(1, len(prices))
            if prices[i - 1]
        ]
        if not returns:
            return 0.0
        mean = sum(returns) / len(returns)
        variance = sum((r - mean) ** 2 for r in returns) / len(returns)
        return math.sqrt(variance)

    def compute_z_score(self) -> float:
        if len(self.prices) < 3:
            return 0.0
        prices = list(self.prices)
        mean = sum(prices) / len(prices)
        variance = sum((p - mean) ** 2 for p in prices) / len(prices)
        std = math.sqrt(variance) if variance > 0 else 1e-9
        return (prices[-1] - mean) / std

    def compute_volume_ratio(self) -> float:
        if not self.volumes:
            return 1.0
        volumes = list(self.volumes)
        avg = sum(volumes[:-1]) / max(len(volumes) - 1, 1) if len(volumes) > 1 else volumes[-1]
        return volumes[-1] / avg if avg else 1.0

    def compute_all(self, current_price: float, current_volume: int) -> dict[str, float]:
        self.add(current_price, current_volume)
        ret_1m, ret_5m = self.compute_returns()
        return {
            "return_1m": ret_1m,
            "return_5m": ret_5m,
            "volatility": self.compute_volatility(),
            "z_score": self.compute_z_score(),
            "volume_ratio": self.compute_volume_ratio(),
            "price": current_price,
        }
