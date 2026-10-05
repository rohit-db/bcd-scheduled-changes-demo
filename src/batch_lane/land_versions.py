"""Land the next slice of BCD-schema booking versions as a JSON-lines file in a UC volume.

Stands in for Kinesis Firehose -> S3 for the demo's batch lane; in production the pipeline's bronze
table would read the Kinesis stream directly. A small cursor file keeps successive runs contiguous.
"""
import argparse
import json
import os
import sys
import time

sys.path.insert(0, os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(sys._getframe().f_code.co_filename)), "..")))
from trip_intel.batch import landing_batch  # noqa: E402


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--landing-path", required=True, help="/Volumes/<catalog>/<schema>/<volume>/pnr_versions")
    p.add_argument("--count", type=int, default=10800, help="versions per run (10800 = 12 min at 15/s)")
    args = p.parse_args()
    os.makedirs(args.landing_path, exist_ok=True)
    cursor_file = os.path.join(os.path.dirname(args.landing_path.rstrip("/")), "_landing_cursor.json")
    start = json.load(open(cursor_file))["next_seq"] if os.path.exists(cursor_file) else 0
    lines, nxt = landing_batch(start, args.count)
    out = os.path.join(args.landing_path, f"pnr_versions_{int(time.time())}_{start}.jsonl")
    with open(out, "w") as f:
        f.write("\n".join(lines) + "\n")
    with open(cursor_file, "w") as f:
        json.dump({"next_seq": nxt}, f)
    print(f"landed {len(lines)} versions (seq {start}..{nxt - 1}) -> {out}")


if __name__ == "__main__":
    main()
