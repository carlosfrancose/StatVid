from statvid.features.feature_engineering import engineer_features


def test_engineer_features_stub():
    out = engineer_features({})
    assert isinstance(out, dict)

