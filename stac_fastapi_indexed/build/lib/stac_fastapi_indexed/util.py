from datetime import datetime, timezone


def utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)
