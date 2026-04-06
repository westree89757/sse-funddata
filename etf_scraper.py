#!/usr/bin/env python3
"""
上交所 ETF 数据爬虫工具
- 获取 ETF 规模数据
- 筛选沪深300ETF相关基金
- 生成柱状图可视化
"""

import os
import re
import json
import time
from datetime import datetime
from typing import List, Dict, Optional

import pandas as pd
import matplotlib.pyplot as plt
import requests

# 设置中文字体
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS', 'SimHei', 'STHeiti']
plt.rcParams['axes.unicode_minus'] = False

# API 配置
ETF_SCALE_API = "https://query.sse.com.cn/commonQuery.do"
ETF_VOLUMN_API = "https://query.sse.com.cn/market/funddata/volumn/queryVolumn.do"

HEADERS = {
    "Referer": "https://www.sse.com.cn/",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/javascript, */*; q=0.01",
}

OUTPUT_DIR = "./output"


def fetch_etf_scale_data() -> Optional[pd.DataFrame]:
    """
    获取上交所 ETF 规模数据 (etfvolumn)
    sqlId: COMMON_SSE_ZQPZ_ETFZL_XXPL_ETFGM_SEARCH_L

    Returns:
        包含 ETF 规模数据的 DataFrame
    """
    print("=" * 60)
    print("上交所 ETF 规模数据爬虫")
    print("=" * 60)

    # 尝试多个可能的 sqlId
    sql_ids = [
        "COMMON_SSE_ZQPZ_ETFZL_XXPL_ETFGM_SEARCH_L",  # ETF规模
        "COMMON_SSE_ZQPZ_ETFZL_XXPL_ETFCJMX_SEARCH_L",  # ETF成交明细
    ]

    for sql_id in sql_ids:
        print(f"\n[INFO] 尝试 sqlId: {sql_id}")

        params = {
            "jsonCallBack": f"jsonpCallback_{int(time.time())}",
            "isPagination": "true",
            "pageHelp.pageSize": "1000",
            "pageHelp.pageNo": "1",
            "pageHelp.beginPage": "1",
            "pageHelp.cacheSize": "1",
            "pageHelp.endPage": "100",
            "sqlId": sql_id,
            "STAT_DATE": "",
            "_": str(int(time.time() * 1000)),
        }

        try:
            response = requests.get(
                ETF_SCALE_API,
                params=params,
                headers=HEADERS,
                timeout=30
            )
            response.raise_for_status()

            # 解析 JSONP
            text = response.text
            match = re.search(r'jsonpCallback_\d+\((.+)\)', text)
            if not match:
                print(f"  [WARN] 无法解析 JSONP 格式")
                continue

            data = json.loads(match.group(1))
            records = data.get("pageHelp", {}).get("data", [])

            if records:
                print(f"  [SUCCESS] 获取到 {len(records)} 条记录")
                df = pd.DataFrame(records)
                print(f"  [INFO] 数据日期: {df['STAT_DATE'].unique() if 'STAT_DATE' in df.columns else 'N/A'}")
                return df

        except requests.RequestException as e:
            print(f"  [ERROR] 请求失败: {e}")
        except json.JSONDecodeError as e:
            print(f"  [ERROR] JSON 解析失败: {e}")

    return None


def fetch_etf_trading_data() -> Optional[pd.DataFrame]:
    """
    获取上交所 ETF 成交数据 (tcuvolumn)
    基于用户提供的 API URL

    Returns:
        包含 ETF 成交数据的 DataFrame
    """
    print("\n" + "=" * 60)
    print("上交所 ETF 成交数据爬虫 (交易型货币基金)")
    print("=" * 60)

    # 用户提供的原始 API
    api_url = "https://query.sse.com.cn/commonQuery.do"
    sql_id = "COMMON_SSE_ZQPZ_ETFZL_XXPL_ETFGM_SEARCH_L"

    params = {
        "jsonCallBack": f"jsonpCallback_{int(time.time())}",
        "isPagination": "true",
        "pageHelp.pageSize": "500",
        "pageHelp.pageNo": "1",
        "pageHelp.beginPage": "1",
        "pageHelp.cacheSize": "1",
        "pageHelp.endPage": "5",
        "sqlId": sql_id,
        "STAT_DATE": "",
        "_": str(int(time.time() * 1000)),
    }

    for attempt in range(2):
        try:
            print(f"\n[INFO] 正在访问上交所 API (第 {attempt + 1} 次)...")
            response = requests.get(api_url, params=params, headers=HEADERS, timeout=30)
            response.raise_for_status()

            text = response.text
            match = re.search(r'jsonpCallback_\d+\((.+)\)', text)
            if not match:
                print(f"  [ERROR] 无法解析 JSONP 格式")
                return None

            data = json.loads(match.group(1))
            records = data.get("pageHelp", {}).get("data", [])

            if records:
                print(f"  [SUCCESS] 获取到 {len(records)} 条记录")
                df = pd.DataFrame(records)

                # 打印字段信息
                print(f"  [INFO] 可用字段: {list(df.columns)}")
                return df

        except requests.RequestException as e:
            print(f"  [ERROR] 请求失败: {e}")
            if attempt < 1:
                print("  [INFO] 准备重试...")
                time.sleep(2)

    return None


def convert_numeric(df: pd.DataFrame) -> pd.DataFrame:
    """转换数值字段为数字类型"""
    numeric_fields = ["TOT_VOL", "TRADING_AMOUNT", "TRADING_VOLUME", "VOL"]
    for col in numeric_fields:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def filter_hs300_etf(df: pd.DataFrame) -> pd.DataFrame:
    """
    筛选沪深300ETF相关基金

    Args:
        df: 原始 ETF 数据

    Returns:
        筛选后的 DataFrame
    """
    if df is None or df.empty:
        return pd.DataFrame()

    # 检查SEC_NAME列是否存在
    if "SEC_NAME" not in df.columns:
        print("[ERROR] 数据中缺少 SEC_NAME 字段")
        print(f"[INFO] 可用字段: {list(df.columns)}")
        return pd.DataFrame()

    # 筛选沪深300ETF相关基金 (名称以"沪深300"开头或包含"沪深300ETF")
    pattern = r"^(沪深300|HS300|hs300)"
    hs300_df = df[df["SEC_NAME"].str.match(pattern, case=False, na=False)].copy()

    # 也包含名称中包含"300ETF"的
    additional = df[df["SEC_NAME"].str.contains(r"300ETF|300E", case=False, na=False)].copy()
    hs300_df = pd.concat([hs300_df, additional]).drop_duplicates()

    print(f"\n[INFO] 找到 {len(hs300_df)} 只沪深300ETF相关基金:")
    print(hs300_df[["SEC_CODE", "SEC_NAME", "STAT_DATE"]].to_string(index=False))

    return hs300_df


def create_bar_chart(df: pd.DataFrame, title: str, output_path: str):
    """
    创建柱状图

    Args:
        df: 数据 DataFrame，需包含 SEC_NAME 和某个数值字段
        title: 图表标题
        output_path: 输出文件路径
    """
    if df is None or df.empty:
        print("[WARN] 无数据可绘图")
        return

    # 确定数值字段
    value_field = None
    for field in ["TOT_VOL", "TRADING_AMOUNT", "TRADING_VOLUME", "VOL"]:
        if field in df.columns:
            value_field = field
            break

    if not value_field:
        print(f"[ERROR] 未找到数值字段，可用字段: {list(df.columns)}")
        return

    # 排序并取前20
    df_plot = df.nlargest(min(20, len(df)), value_field).copy()

    # 转换数值为浮点数
    df_plot[value_field] = pd.to_numeric(df_plot[value_field], errors="coerce")

    # 创建图表
    fig, ax = plt.subplots(figsize=(14, 8))

    bars = ax.barh(
        range(len(df_plot)),
        df_plot[value_field].values,
        color="steelblue",
        edgecolor="white",
        height=0.7
    )

    # 设置标签
    ax.set_yticks(range(len(df_plot)))
    ax.set_yticklabels(df_plot["SEC_NAME"].values)
    ax.invert_yaxis()  # 最高的在顶部

    # 添加数值标签
    for i, (bar, val) in enumerate(zip(bars, df_plot[value_field].values)):
        ax.text(
            val + df_plot[value_field].max() * 0.01,
            i,
            f"{val:,.2f}",
            va="center",
            fontsize=9
        )

    # 设置标题和标签
    ax.set_title(title, fontsize=14, fontweight="bold", pad=20)
    ax.set_xlabel(value_field, fontsize=11)
    ax.grid(axis="x", alpha=0.3, linestyle="--")

    # 调整布局
    plt.tight_layout()

    # 保存图表
    plt.savefig(output_path, dpi=150, bbox_inches="tight", facecolor="white")
    print(f"[INFO] 图表已保存: {output_path}")
    plt.close()


def save_to_csv(df: pd.DataFrame, filename: str):
    """
    保存数据到 CSV

    Args:
        df: 数据 DataFrame
        filename: 文件名
    """
    if df is None or df.empty:
        return

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    filepath = os.path.join(OUTPUT_DIR, filename)
    df.to_csv(filepath, index=False, encoding="utf-8-sig")
    print(f"[INFO] 数据已保存: {filepath}")


def print_summary(df: pd.DataFrame, label: str):
    """
    打印数据摘要

    Args:
        df: 数据 DataFrame
        label: 数据标签
    """
    if df is None or df.empty:
        print(f"\n[{label}] 无数据")
        return

    print(f"\n{'=' * 60}")
    print(f"{label} 数据概览")
    print(f"{'=' * 60}")
    print(f"总记录数: {len(df)}")
    print(f"日期范围: {df['STAT_DATE'].unique() if 'STAT_DATE' in df.columns else 'N/A'}")

    # 打印成交金额/规模最大的前10只
    value_field = None
    for field in ["TOT_VOL", "TRADING_AMOUNT", "TRADING_VOLUME"]:
        if field in df.columns:
            value_field = field
            break

    if value_field:
        df_sorted = df.nlargest(10, value_field).copy()
        df_sorted[value_field] = pd.to_numeric(df_sorted[value_field], errors="coerce")
        print(f"\nTOP 10 {value_field}:")
        print(df_sorted[["SEC_CODE", "SEC_NAME", value_field]].to_string(index=False))


def main():
    """主流程"""
    print("\n" + "#" * 60)
    print("#  上交所 ETF 数据爬虫工具")
    print("#" * 60)

    # 1. 尝试获取 ETF 成交数据
    df_trading = fetch_etf_trading_data()

    if df_trading is not None and not df_trading.empty:
        df_trading = convert_numeric(df_trading)
        print_summary(df_trading, "ETF成交数据")

        # 保存全部数据
        save_to_csv(df_trading, "etf_trading_all.csv")

        # 筛选沪深300ETF
        df_hs300 = filter_hs300_etf(df_trading.copy())
        if not df_hs300.empty:
            save_to_csv(df_hs300, "etf_hs300_filtered.csv")
            create_bar_chart(
                df_hs300,
                "沪深300ETF相关基金规模",
                os.path.join(OUTPUT_DIR, "etf_hs300_bar.png")
            )
    else:
        print("\n[WARN] 无法获取ETF成交数据")

    # 2. 也尝试获取 ETF 规模数据
    df_scale = fetch_etf_scale_data()

    if df_scale is not None and not df_scale.empty:
        df_scale = convert_numeric(df_scale)
        print_summary(df_scale, "ETF规模数据")

        # 保存全部数据
        save_to_csv(df_scale, "etf_scale_all.csv")

        # 筛选沪深300ETF
        df_hs300 = filter_hs300_etf(df_scale.copy())
        if not df_hs300.empty:
            save_to_csv(df_hs300, "etf_scale_hs300.csv")
            create_bar_chart(
                df_hs300,
                "沪深300ETF规模分布",
                os.path.join(OUTPUT_DIR, "etf_scale_hs300_bar.png")
            )
    else:
        print("\n[WARN] 无法获取ETF规模数据")

    print("\n" + "#" * 60)
    print("#  爬虫执行完成!")
    print("#" * 60)


if __name__ == "__main__":
    main()
