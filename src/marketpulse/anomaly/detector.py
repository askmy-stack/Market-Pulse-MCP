"""Anomaly detection engine."""

from __future__ import annotations

from marketpulse.config import get_settings
from marketpulse.schemas.events import AnomalySeverity, StockAnomalyEvent, StockFeatureEvent


class AnomalyDetector:
    def __init__(self):
        self._settings = get_settings()

    def _severity(self, z_score: float, volume_ratio: float) -> AnomalySeverity:
        score = abs(z_score) + max(0, volume_ratio - 1)
        if score >= 5:
            return AnomalySeverity.CRITICAL
        if score >= 4:
            return AnomalySeverity.HIGH
        if score >= 3:
            return AnomalySeverity.MEDIUM
        return AnomalySeverity.LOW

    def detect(self, features: StockFeatureEvent) -> StockAnomalyEvent | None:
        z = features.z_score
        vol_ratio = features.volume_ratio
        threshold = self._settings.anomaly_zscore_threshold
        volume_spike = self._settings.volume_spike_ratio

        is_price_anomaly = abs(z) >= threshold
        is_volume_spike = vol_ratio >= volume_spike

        if not (is_price_anomaly or is_volume_spike):
            return None

        types = []
        if is_price_anomaly:
            types.append("price_zscore")
        if is_volume_spike:
            types.append("volume_spike")

        severity = self._severity(z, vol_ratio)
        direction = "up" if features.return_1m >= 0 else "down"
        description = (
            f"{features.symbol} showed unusual {direction}ward activity: "
            f"z-score={z:.2f}, volume_ratio={vol_ratio:.2f}x"
        )

        return StockAnomalyEvent(
            symbol=features.symbol,
            timestamp=features.timestamp,
            anomaly_type="+".join(types),
            severity=severity,
            z_score=z,
            volume_ratio=vol_ratio,
            price=features.price,
            price_change_pct=features.return_1m * 100,
            description=description,
        )
