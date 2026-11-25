import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

os.environ["SKIP_PREFECT"] = "true"

from flows.updaters.update_tolid_prefixes import update_tolid_prefixes  # noqa: E402


def test_update_tolid_prefixes_returns_success_when_refresh_succeeds(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "flows.updaters.update_tolid_prefixes.tolid_file_is_up_to_date",
        lambda *args, **kwargs: False,
    )
    monkeypatch.setattr(
        "flows.updaters.update_tolid_prefixes.fetch_tolid_prefixes",
        lambda local_path, http_path: (True, 400_001),
    )
    monkeypatch.setattr(
        "flows.updaters.update_tolid_prefixes.emit_event",
        lambda **kwargs: None,
    )

    assert update_tolid_prefixes(str(tmp_path)) is True
