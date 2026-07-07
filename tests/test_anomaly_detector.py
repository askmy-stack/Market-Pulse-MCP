"""Test anomaly detector."""

from datetime import datetime

from marketpulse.anomaly.detector import AnomalyDetector
from marketpulse.schemas.events import AnomalySeverity, StockFeatureEvent


def test_detects_price_anomaly():
    detector = AnomalyDetector()
    features = StockFeatureEvent(
        symbol="AAPL",
        timestamp=datetime.utcnow(),
        z_score=3.5,
        volume_ratio=1.2,
        return_1m=0.03,
        price=195.0,
    )
    anomaly = detector.detect(features)
    assert anomaly is not None
    assert anomaly.severity in {
        AnomalySeverity.MEDIUM,
        AnomalySeverity.HIGH,
        AnomalySeverity.CRITICAL,
    }
    assert "price_zscore" in anomaly.anomaly_type


def test_detects_volume_spike():
    detector = AnomalyDetector()
    features = StockFeatureEvent(
        symbol="MSFT",
        timestamp=datetime.utcnow(),
        z_score=0.5,
        volume_ratio=3.0,
        return_1m=0.001,
        price=420.0,
    )
    anomaly = detector.detect(features)
    assert anomaly is not None
    assert "volume_spike" in anomaly.anomaly_type


def test_no_anomaly_normal_conditions():
    detector = AnomalyDetector()
    features = StockFeatureEvent(
        symbol="GOOGL",
        timestamp=datetime.utcnow(),
        z_score=0.3,
        volume_ratio=1.1,
        return_1m=0.001,
        price=175.0,
    )
    assert detector.detect(features) is None
