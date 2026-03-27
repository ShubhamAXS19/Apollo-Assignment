#!/usr/bin/env bash
# =============================================================================
# cron_setup.sh — Pipeline scheduling config  (GAP 6 FIX)
#
# Two options provided:
#   Option A: crontab entry  (simplest, works on any Linux/macOS)
#   Option B: systemd timer  (production-grade, auto-restarts on failure)
#
# Usage:
#   chmod +x cron_setup.sh
#   ./cron_setup.sh          # installs crontab entry (Option A)
#   ./cron_setup.sh systemd  # installs systemd service (Option B)
# =============================================================================

set -euo pipefail

PIPELINE_DIR="$(cd "$(dirname "$0")" && pwd)"
PYTHON="${PIPELINE_DIR}/.venv/bin/python"
LOG_DIR="${PIPELINE_DIR}/logs"
BACKEND="${SMS_BACKEND:-duckdb}"   # override via env: SMS_BACKEND=postgres ./cron_setup.sh

mkdir -p "$LOG_DIR"

# ─────────────────────────────────────────────────────────────────────────────
# OPTION A: crontab
# Runs the full pipeline at 02:00 AM IST (20:30 UTC) every day.
# ─────────────────────────────────────────────────────────────────────────────

install_cron() {
    CRON_JOB="30 20 * * * cd ${PIPELINE_DIR} && ${PYTHON} run_full_pipeline.py --backend ${BACKEND} >> ${LOG_DIR}/pipeline_\$(date +\%Y\%m\%d).log 2>&1"

    # Add only if not already present
    (crontab -l 2>/dev/null | grep -v "run_full_pipeline"; echo "$CRON_JOB") | crontab -

    echo "✅ Crontab entry installed:"
    echo "   ${CRON_JOB}"
    echo ""
    echo "Verify with: crontab -l"
}

# ─────────────────────────────────────────────────────────────────────────────
# OPTION B: systemd timer  (recommended for production Linux servers)
# ─────────────────────────────────────────────────────────────────────────────

install_systemd() {
    SERVICE_FILE="/etc/systemd/system/sms-pipeline.service"
    TIMER_FILE="/etc/systemd/system/sms-pipeline.timer"

    sudo tee "$SERVICE_FILE" > /dev/null <<EOF
[Unit]
Description=SMS Analytics Pipeline — borrower feature refresh
After=network.target postgresql.service
Wants=postgresql.service

[Service]
Type=oneshot
User=$(whoami)
WorkingDirectory=${PIPELINE_DIR}
ExecStart=${PYTHON} run_full_pipeline.py --backend ${BACKEND}
StandardOutput=append:${LOG_DIR}/pipeline.log
StandardError=append:${LOG_DIR}/pipeline_errors.log
# Retry up to 3 times on failure, 5 min apart
Restart=on-failure
RestartSec=300
StartLimitIntervalSec=900
StartLimitBurst=3

[Install]
WantedBy=multi-user.target
EOF

    sudo tee "$TIMER_FILE" > /dev/null <<EOF
[Unit]
Description=Daily SMS pipeline run at 02:00 IST (20:30 UTC)

[Timer]
OnCalendar=*-*-* 20:30:00 UTC
Persistent=true       ; run immediately if last run was missed (e.g. server was off)
RandomizedDelaySec=5m ; jitter to avoid thundering-herd if many servers run this

[Install]
WantedBy=timers.target
EOF

    sudo systemctl daemon-reload
    sudo systemctl enable --now sms-pipeline.timer

    echo "✅ systemd timer installed and started."
    echo "   Check status : systemctl status sms-pipeline.timer"
    echo "   View logs    : journalctl -u sms-pipeline.service -f"
    echo "   Manual run   : systemctl start sms-pipeline.service"
}

# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

if [[ "${1:-}" == "systemd" ]]; then
    install_systemd
else
    install_cron
fi