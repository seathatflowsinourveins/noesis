#!/usr/bin/env bash
# Run in Noesis-SOTA after a successful paper-farm cycle has published the Parquet files.
set -euo pipefail
repo_root="$(cd -- "$(dirname -- "$0")/.." && pwd)"
ui_dir=/mnt/z/sota-data/manifests/paper_farm_ui
for lane in pa1 pa2 pa3; do
    tmp_db="$ui_dir/.${lane}.duckdb.$$.tmp"
    rm -f "$tmp_db"
    for ledger in paper_wire_orders paper_tca_fills; do
        parquet="$ui_dir/alpaca_${lane}_${ledger}.parquet"
        test -f "$parquet"
        duckdb "$tmp_db" -bail -c \
            "CREATE OR REPLACE VIEW $ledger AS SELECT * FROM read_parquet('$parquet')"
    done
    mv -f "$tmp_db" "$ui_dir/$lane.duckdb"
done
tv_parquet=/mnt/z/sota-data/manifests/tv_sota_pack/tv_staged_presets.parquet
if [[ -f "$tv_parquet" ]]; then
    tmp_tv="$ui_dir/.tv_audit.duckdb.$$.tmp"
    rm -f "$tmp_tv"
    duckdb "$tmp_tv" -bail -c \
        "CREATE OR REPLACE VIEW tv_staged_presets AS SELECT * FROM read_parquet('$tv_parquet')"
    mv -f "$tmp_tv" "$ui_dir/tv_audit.duckdb"
fi
sudo install -Dm644 "$repo_root/docker/duckdb-ui/paper-farm.conf" \
    /etc/systemd/system/tier0-duckdb-ui.service.d/paper-farm.conf
sudo systemctl daemon-reload
sudo systemctl restart tier0-duckdb-ui.service
