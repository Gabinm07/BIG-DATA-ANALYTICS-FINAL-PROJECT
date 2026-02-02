import json, glob
import happybase

HBASE_HOST = "localhost"
HBASE_PORT = 9090
TABLE = "user_sessions"

def b(x):
    if x is None:
        return b""
    if isinstance(x, (dict, list)):
        return json.dumps(x).encode("utf-8")
    return str(x).encode("utf-8")

conn = happybase.Connection(host=HBASE_HOST, port=HBASE_PORT)
table = conn.table(TABLE)

files = sorted(glob.glob("sessions_*.json"))
print("Found:", files)

batch = table.batch(batch_size=1000)
count = 0

for fp in files:
    print("Loading:", fp)
    with open(fp, "r", encoding="utf-8") as f:
        sessions = json.load(f)

    for s in sessions:
        rowkey = f"{s['user_id']}#{s['start_time']}#{s['session_id']}".encode("utf-8")

        batch.put(rowkey, {
            b"meta:user_id": b(s.get("user_id")),
            b"meta:session_id": b(s.get("session_id")),
            b"meta:start_time": b(s.get("start_time")),
            b"meta:end_time": b(s.get("end_time")),
            b"meta:duration_seconds": b(s.get("duration_seconds")),
            b"meta:conversion_status": b(s.get("conversion_status")),
            b"meta:referrer": b(s.get("referrer")),
            b"meta:device_profile": b(s.get("device_profile")),
            b"meta:geo_data": b(s.get("geo_data")),
            b"pv:page_views": b(s.get("page_views")),
            b"cart:cart_contents": b(s.get("cart_contents")),
        })

        count += 1
        if count % 20000 == 0:
            print("Inserted:", count)

batch.send()
conn.close()
print("DONE. Total inserted:", count)
