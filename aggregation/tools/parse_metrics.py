import re
import sys

"""Summarize Metrics sampler lines from a server.log file.

Usage:
    python3 parse_metrics.py server.log
"""


def main():
    path = sys.argv[1] if len(sys.argv) > 1 else "server.log"
    fields = {
        "samples": 0,
        "req_qps_sum": 0,
        "avg60_sum": 0,
        "req_avg_sum": 0.0,
        "cpu_sum": 0,
        "rss_sum": 0,
        "cpu_max": 0,
        "rss_max": 0,
        "queue_max": 0,
        "conns_min": 10**9,
        "conns_max": 0,
        "err_sum": 0,
        "p99_1ms": 0,
        "p99_5ms": 0,
        "p99_10ms": 0,
        "bp_d_size_max": 0,
        "bp_w_size_max": 0,
        "bp_db_size_max": 0,
        "bp_d_full_max": 0,
        "bp_w_full_max": 0,
        "bp_db_full_max": 0,
        "db_queue_max": 0,
        "db_wait_max": 0,
        "credit_min": 10**9,
        "db_active_max": 0,
    }

    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            if "req_qps=" not in line:
                continue
            q = re.search(r"req_qps=(\d+)", line)
            if not q or int(q.group(1)) == 0:
                continue
            fields["samples"] += 1
            fields["req_qps_sum"] += int(q.group(1))
            a60 = re.search(r"req_avg60=(\d+)", line)
            if a60:
                fields["avg60_sum"] += int(a60.group(1))
            avg = re.search(r"req_avg=([0-9.]+)ms", line)
            if avg:
                fields["req_avg_sum"] += float(avg.group(1))
            cpu = re.search(r"cpu=(\d+)%", line)
            if cpu:
                v = int(cpu.group(1))
                fields["cpu_sum"] += v
                fields["cpu_max"] = max(fields["cpu_max"], v)
            rss = re.search(r"rss=(\d+)KB", line)
            if rss:
                v = int(rss.group(1))
                fields["rss_sum"] += v
                fields["rss_max"] = max(fields["rss_max"], v)
            queue = re.search(r"queue\(d/w/db\)=(\d+)/(\d+)/(\d+)", line)
            if queue:
                v = max(int(queue.group(1)), int(queue.group(2)), int(queue.group(3)))
                fields["queue_max"] = max(fields["queue_max"], v)
            conns = re.search(r"conns=(\d+)", line)
            if conns:
                v = int(conns.group(1))
                fields["conns_max"] = max(fields["conns_max"], v)
                fields["conns_min"] = min(fields["conns_min"], v)
            err = re.search(r"err\(d/w/db\)=(\d+)/(\d+)/(\d+)", line)
            if err:
                fields["err_sum"] += int(err.group(1)) + int(err.group(2)) + int(err.group(3))
            p99 = re.search(r"req_p99=([0-9.]+)ms", line)
            if p99:
                v = float(p99.group(1))
                if v <= 1:
                    fields["p99_1ms"] += 1
                elif v <= 5:
                    fields["p99_5ms"] += 1
                elif v <= 10:
                    fields["p99_10ms"] += 1
            bp = re.search(
                r"bp\(d/w/db\)=(\d+)/(\d+)/(\d+)\|"
                r"(\d+)/(\d+)/(\d+)\|(\d+)/(\d+)/(\d+)", line)
            if bp:
                fields["bp_d_size_max"] = max(
                    fields["bp_d_size_max"], int(bp.group(1)))
                fields["bp_w_size_max"] = max(
                    fields["bp_w_size_max"], int(bp.group(4)))
                fields["bp_db_size_max"] = max(
                    fields["bp_db_size_max"], int(bp.group(7)))
                fields["bp_d_full_max"] = max(
                    fields["bp_d_full_max"], int(bp.group(3)))
                fields["bp_w_full_max"] = max(
                    fields["bp_w_full_max"], int(bp.group(6)))
                fields["bp_db_full_max"] = max(
                    fields["bp_db_full_max"], int(bp.group(9)))
            dbinfo = re.search(
                r"db\(queue=(\d+)/(\d+)/(\d+)/(\d+) wait=(\d+) "
                r"credit=(\d+)/(\d+) active=(\d+)\)", line)
            if dbinfo:
                fields["db_queue_max"] = max(
                    fields["db_queue_max"], int(dbinfo.group(1)))
                fields["db_wait_max"] = max(
                    fields["db_wait_max"], int(dbinfo.group(5)))
                fields["credit_min"] = min(
                    fields["credit_min"], int(dbinfo.group(6)))
                fields["db_active_max"] = max(
                    fields["db_active_max"], int(dbinfo.group(8)))

    n = fields["samples"]
    if not n:
        print("no active metric samples")
        return
    print(
        "active_samples=%d req_qps_avg=%.0f avg60_avg=%.0f "
        "req_avg_avg=%.3fms cpu_avg=%.0f%% cpu_max=%d%% "
        "rss_avg=%.0fKB rss_max=%dKB queue_max=%d conns=%d-%d "
        "err_sum=%d p99_1ms=%d p99_5ms=%d p99_10ms=%d "
        "bp_d_size_max=%d bp_w_size_max=%d bp_db_size_max=%d "
        "full_max(d/w/db)=%d/%d/%d db_queue_max=%d "
        "db_wait_max=%d credit_min=%d db_active_max=%d"
        % (
            n,
            fields["req_qps_sum"] / n,
            fields["avg60_sum"] / n,
            fields["req_avg_sum"] / n,
            fields["cpu_sum"] / n,
            fields["cpu_max"],
            fields["rss_sum"] / n,
            fields["rss_max"],
            fields["queue_max"],
            fields["conns_min"],
            fields["conns_max"],
            fields["err_sum"],
            fields["p99_1ms"],
            fields["p99_5ms"],
            fields["p99_10ms"],
            fields["bp_d_size_max"],
            fields["bp_w_size_max"],
            fields["bp_db_size_max"],
            fields["bp_d_full_max"],
            fields["bp_w_full_max"],
            fields["bp_db_full_max"],
            fields["db_queue_max"],
            fields["db_wait_max"],
            fields["credit_min"],
            fields["db_active_max"],
        )
    )


if __name__ == "__main__":
    main()
