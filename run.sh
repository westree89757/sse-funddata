#!/bin/bash
# 上交所 ETF 数据定时抓取脚本
# 使用方法: ./run.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

echo "========================================"
echo "上交所 ETF 数据抓取 - $(date '+%Y-%m-%d %H:%M:%S')"
echo "========================================"

# 检查依赖
if ! command -v python3 &> /dev/null; then
    echo "[ERROR] 未找到 python3，请先安装 Python"
    exit 1
fi

# 安装依赖（如需要）
pip3 install -q pandas requests matplotlib 2>/dev/null || true

# 运行爬虫
python3 etf_scraper.py

echo ""
echo "[$(date '+%Y-%m-%d %H:%M:%S')] 抓取完成!"
echo "数据目录: $SCRIPT_DIR/output/"
