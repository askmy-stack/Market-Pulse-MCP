"""Test rolling feature calculations."""

from marketpulse.features.rolling_features import RollingWindow


def test_rolling_window_returns():
    window = RollingWindow(window_size=10)
    prices = [100, 101, 102, 103, 104]
    for i, p in enumerate(prices):
        features = window.compute_all(p, 1000 + i * 100)
    assert features["return_1m"] > 0
    assert features["price"] == 104


def test_rolling_window_z_score():
    window = RollingWindow(window_size=20)
    for p in [100.0] * 19:
        window.compute_all(p, 1000)
    features = window.compute_all(110.0, 5000)
    assert abs(features["z_score"]) > 1


def test_volume_ratio_spike():
    window = RollingWindow(window_size=10)
    for _ in range(5):
        window.compute_all(100.0, 1000)
    features = window.compute_all(100.0, 10000)
    assert features["volume_ratio"] > 1.5
