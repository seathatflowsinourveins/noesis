#!/usr/bin/env bash
# Run in Noesis-SOTA after a successful paper-farm cycle has published the Parquet files.
set -euo pipefail
repo_root="$(cd -- "$(dirname -- "$0")/.." && pwd)"
ui_dir=/mnt/z/sota-data/manifests/paper_farm_ui
for lane in pa1 pa2 pa3; do
    for ledger in paper_wire_orders paper_tca_fills; do
        parquet="$ui_dir/alpaca_${lane}_${ledger}.parquet"
        test -f "$parquet"
        duckdb "$ui_dir/$lane.duckdb" -bail -c \
            "CREATE OR REPLACE VIEW $ledger AS SELECT * FROM read_parquet('$parquet')"
    done
done
sudo install -Dm644 "$repo_root/docker/duckdb-ui/paper-farm.conf" \
    /etc/systemd/system/tier0-duckdb-ui.service.d/paper-farm.conf
sudo systemctl daemon-reload
sudo systemctl restart tier0-duckdb-ui.service
