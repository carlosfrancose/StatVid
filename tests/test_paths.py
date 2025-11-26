from statvid.utils.paths import get_paths


def test_paths_shape():
    p = get_paths()
    assert p.bronze and p.silver and p.gold

