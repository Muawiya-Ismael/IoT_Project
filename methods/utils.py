import datetime

def parse_iso_to_utc_naive(s):
    try:
        dt = datetime.datetime.fromisoformat(s.replace('Z', '+00:00'))
        if dt.tzinfo is not None:
            dt = dt.astimezone(datetime.timezone.utc).replace(tzinfo=None)
        return dt
    except Exception:
        return None


def minute_floor(dt):
    return dt.replace(second=0, microsecond=0)
