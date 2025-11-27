#!/usr/bin/env bash
set -e

#
# Script to fetch and format mongo-shake stats information for the incremental replication phase
#
# Usage: mongo-shake-stats.sh <mongo_shake_url>

# Default URL if not provided
MONGO_SHAKE_URL=${1:-"http://localhost:9100"}

# Remove trailing slash if present
MONGO_SHAKE_URL=${MONGO_SHAKE_URL%/}

# Fetch replication state
# Example JSON:
# {"who":"mongoshake","tag":",b05864e209f3901fc026189452e0a5c0214d7bdf,release,go1.18.3,2025-06-30_15:03:46","replset":"rs0","logs_get":152235,"logs_repl":85447,"logs_success":85447,"tps":0,"lsn":{"unix":1751998421,"time":"2025-07-08 18:13:41","ts":"7524775920838639618"},"lsn_ack":{"unix":1751998421,"time":"2025-07-08 18:13:41","ts":"7524775920838639618"},"lsn_ckpt":{"unix":1751999717,"time":"2025-07-08 18:35:17","ts":"7524781487116255233"},"now":{"unix":1751999800,"time":"2025-07-08 18:36:40"},"log_size_avg":"466.00B","log_size_max":"4.21KB"}
# Example table format:

REPL_RESPONSE=$(curl -s "$MONGO_SHAKE_URL/repl")

# Display replication status table
echo
echo "Replication status:"
echo

(
  echo "$REPL_RESPONSE" | \
    jq -r '{"who": .who, "tag": .tag, "replset": .replset, "logs_get": .logs_get, "logs_repl": .logs_repl, "logs_success": .logs_success, "tps": .tps, "log_size_avg": .log_size_avg, "log_size_max": .log_size_max} | to_entries | .[] | "\(.key) | \(.value)"'
) | column -t -s'|' || echo "Error: Failed to parse metadata"

# lsn.ts                 lsn.ts_unix    lsn.ts_unix
# 7524775920838639618    1751998421     2025-07-08 18:13:41
# 7524775920838639618    1751998421     2025-07-08 18:13:41
# 7524783870823104513    1752000272     2025-07-08 18:44:32

echo
echo "LSN Information:"
echo

(
 echo "lsn.ts | lsn.ts_unix | lsn.ts_unix"
 echo "$REPL_RESPONSE" | \
   jq -r '[["lsn", .lsn], ["lsn_ack", .lsn_ack], ["lsn_ckpt", .lsn_ckpt]] | .[] | "\(.[1].ts) | \(.[1].unix) | \(.[1].time)"'
) | column -t -s'|' || echo "Error: Failed to fetch or parse mongo-shake stats"

# Fetch worker state
# Example JSON:
# [{"worker_id":0,"jobs_in_queue":0,"jobs_unack_buffer":0,"last_unack":"7524743755828559875","last_ack":"7524743755828559875","count":65655},{"worker_id":1,"jobs_in_queue":0,"jobs_unack_buffer":0,"last_unack":"7524709039607906306","last_ack":"7524709039607906306","count":43},{"worker_id":2,"jobs_in_queue":0,"jobs_unack_buffer":0,"last_unack":"7524775920838639617","last_ack":"7524775920838639617","count":9889},{"worker_id":3,"jobs_in_queue":0,"jobs_unack_buffer":0,"last_unack":"7524775697500340225","last_ack":"7524775697500340225","count":426},{"worker_id":4,"jobs_in_queue":0,"jobs_unack_buffer":0,"last_unack":"7524723569482268674","last_ack":"7524723569482268674","count":8052},{"worker_id":5,"jobs_in_queue":0,"jobs_unack_buffer":0,"last_unack":"7524775920838639618","last_ack":"7524775920838639618","count":1339},{"worker_id":6,"jobs_in_queue":0,"jobs_unack_buffer":0,"last_unack":"7524517299382910978","last_ack":"7524517299382910978","count":5},{"worker_id":7,"jobs_in_queue":0,"jobs_unack_buffer":0,"last_unack":"7524723775640698881","last_ack":"7524723775640698881","count":38}]
# Example table format
# worker_id    jobs_in_queue    jobs_unack_buffer    last_unack             last_ack               count
# 0            0                0                    7524743755828559875    7524743755828559875    65655
# 1            0                0                    7524709039607906306    7524709039607906306    43
# 2            0                0                    7524775920838639617    7524775920838639617    9889
# 3            0                0                    7524775697500340225    7524775697500340225    426
# 4            0                0                    7524723569482268674    7524723569482268674    8052
echo
echo "Worker Information:"
echo
(
 echo "worker_id | jobs_in_queue | jobs_unack_buffer | last_unack | last_ack | count"
 curl -s "$MONGO_SHAKE_URL/worker" | \
   jq -r '.[] | "\(.worker_id) | \(.jobs_in_queue) | \(.jobs_unack_buffer) | \(.last_unack) | \(.last_ack) | \(.count)"'
) | column -t -s'|' || echo "Error: Failed to fetch or parse mongo-shake worker stats"