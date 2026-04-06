#!/bin/bash
# 停止沪深300ETF数据展示平台

pkill -f web_app.py && echo "已停止 web_app.py" || echo "服务未运行"
