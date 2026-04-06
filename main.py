#!/usr/bin/env python3
"""
上交所 ETF 成交数据抓取工具
SSE ETF Trading Data Scraper
"""

import os
import json
import time
from datetime import datetime
from typing import Optional, Dict, Any

import pandas as pd
import requests


# ==================== 配置区 ====================
BASE_URL = "https://www.sse.com.cn/market/funddata/volumn/tcuvolumn/"
API_URL = "https://query.sse.com.cn/market/funddata/volumn/queryVolumn.do"
OUTPUT_DIR = "./output"
HEADERS = {
    "Referer": "https://www.sse.com.cn/",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
}


# ==================== 核心函数 ====================

def fetch_api_data(max_retries: int = 2) -> Optional[Dict[str, Any]]:
    """
    访问上交所 ETF 成交数据 API

    Args:
        max_retries: 最大重试次数

    Returns:
        API 返回的原始 JSON 数据，失败返回 None
    """
    params = {
        "jsonCallBack": "jsonpCallback",
        "isPagination": "true",
        "pageHelp.pageSize": "500",
        "pageHelp.pageNo": "1",
        "pageHelp.beginPage": "1",
        "pageHelp.endPage": "1",
        "stockType": "8",  # 8 = ETF
    }

    for attempt in range(max_retries):
        try:
            print(f"[INFO] 正在尝试访问上交所 API (第 {attempt + 1} 次)...")
            response = requests.get(API_URL, params=params, headers=HEADERS, timeout=30)
            response.raise_for_status()

            # 处理 JSONP 回调格式: jsonpCallback({...})
            text = response.text
            if text.startswith("jsonpCallback"):
                text = text[len("jsonpCallback("):-1]

            data = json.loads(text)
            print(f"[INFO] 成功获取数据，共 {len(data.get('result', []))} 条记录")
            return data

        except requests.RequestException as e:
            print(f"[ERROR] 请求失败: {e}")
            if attempt < max_retries - 1:
                print("[INFO] 准备重试...")
                time.sleep(2)
            else:
                print("[ERROR] 已达到最大重试次数，放弃获取数据")
                return None
        except json.JSONDecodeError as e:
            print(f"[ERROR] JSON 解析失败: {e}")
            return None


def parse_response(data: Dict[str, Any]) -> pd.DataFrame:
    """
    解析 API 响应数据并转换为 DataFrame

    Args:
        data: API 返回的原始数据字典

    Returns:
        清洗后的 pandas DataFrame
    """
    records = data.get("result", [])
    if not records:
        print("[WARN] 未获取到任何记录")
        return pd.DataFrame()

    df = pd.DataFrame(records)

    # 字段映射 (根据实际 API 返回字段调整)
    # 常见字段名: SECUCODE, SECURITY_CODE, SECURITY_NAME_ABBR, TRADING_VOLUME, TRADING_AMOUNT, TRADING_COUNT
    field_mapping = {
        "SECURITY_CODE": "证券代码",
        "SECURITY_NAME_ABBR": "证券简称",
        "TRADING_VOLUME": "成交数量(万份)",
        "TRADING_AMOUNT": "成交金额(万元)",
        "TRADING_COUNT": "成交笔数",
    }

    # 只保留已知字段
    available_fields = {k: v for k, v in field_mapping.items() if k in df.columns}
    if not available_fields:
        # 如果字段名不匹配，打印实际列名供调试
        print(f"[DEBUG] API 返回的列名: {list(df.columns)}")
        return pd.DataFrame()

    df = df[list(available_fields.keys())].rename(columns=available_fields)

    # 确保数值字段为数字类型
    numeric_fields = ["成交数量(万份)", "成交金额(万元)", "成交笔数"]
    for col in numeric_fields:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # 确保证券代码保持字符串格式（不丢失前导零）
    if "证券代码" in df.columns:
        df["证券代码"] = df["证券代码"].astype(str).str.zfill(6)

    return df


def save_to_csv(df: pd.DataFrame, output_dir: str = OUTPUT_DIR) -> str:
    """
    保存 DataFrame 到 CSV 文件

    Args:
        df: 要保存的数据
        output_dir: 输出目录

    Returns:
        生成的 CSV 文件路径
    """
    os.makedirs(output_dir, exist_ok=True)

    today = datetime.now().strftime("%Y%m%d")
    filename = f"etf_daily_report_{today}.csv"
    filepath = os.path.join(output_dir, filename)

    df.to_csv(filepath, index=False, encoding="utf-8-sig")
    print(f"[INFO] 数据已保存至: {filepath}")
    return filepath


def print_top_etfs(df: pd.DataFrame, n: int = 10) -> None:
    """
    打印成交金额最高的前 N 只 ETF

    Args:
        df: ETF 数据 DataFrame
        n: 显示数量，默认为 10
    """
    if df.empty or "成交金额(万元)" not in df.columns:
        print("[WARN] 数据为空或缺少'成交金额'字段")
        return

    top_etfs = df.nlargest(n, "成交金额(万元)")[[
        "证券代码", "证券简称", "成交金额(万元)", "成交数量(万份)", "成交笔数"
    ]]

    print("\n" + "=" * 80)
    print(f"📊 今日成交金额 TOP {n} ETF 概览")
    print("=" * 80)
    print(top_etfs.to_string(index=False))
    print("=" * 80 + "\n")


# ==================== 主流程 ====================

def main():
    """主执行流程"""
    print("=" * 60)
    print("上交所 ETF 成交数据抓取工具")
    print("=" * 60)

    # Step 1: 获取数据
    raw_data = fetch_api_data()
    if raw_data is None:
        print("[FATAL] 无法获取数据，程序退出")
        return 1

    # Step 2: 解析数据
    df = parse_response(raw_data)
    if df.empty:
        print("[FATAL] 数据解析失败或无数据，程序退出")
        return 1

    # Step 3: 保存 CSV
    save_to_csv(df)

    # Step 4: 打印 TOP 10
    print_top_etfs(df)

    print("[DONE] 抓取任务完成!")
    return 0


if __name__ == "__main__":
    exit(main())
