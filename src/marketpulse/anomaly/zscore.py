"""Z-score utilities."""

from __future__ import annotations

import math


def compute_z_score(value: float, values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean = sum(values) / len(values)
    variance = sum((v - mean) ** 2 for v in values) / len(values)
    std = math.sqrt(variance) if variance > 0 else 1e-9
    return (value - mean) / std
