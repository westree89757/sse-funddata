"""
测试配置
"""

import os
import sys

# 确保项目根目录在路径中
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 测试数据库配置（使用独立的测试数据库）
TEST_CONFIG = {
    'database': {
        'host': '127.0.0.1',
        'port': 3306,
        'username': 'root',
        'password': '12345678',
        'name': 'etf_data_test'  # 测试使用独立数据库
    }
}
