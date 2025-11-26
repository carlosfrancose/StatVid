from statvid.config import get_config


def test_config_defaults():
    cfg = get_config()
    assert cfg.data_dir
    assert cfg.log_level

