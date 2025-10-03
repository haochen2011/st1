#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试新包结构的导入是否正常
"""

def test_imports():
    """测试各个模块的导入"""
    print("🔍 测试模块导入...")

    try:
        # 测试核心模块
        from core.config import Config
        print("✅ core.config 导入成功")
    except ImportError as e:
        print(f"❌ core.config 导入失败: {e}")

    try:
        # 测试数据模块
        from data.database import DatabaseManager
        print("✅ data.database 导入成功")
    except ImportError as e:
        print(f"❌ data.database 导入失败: {e}")

    try:
        # 测试处理器模块
        from processors.basic_processor import BasicProcessor
        print("✅ processors.basic_processor 导入成功")
    except ImportError as e:
        print(f"❌ processors.basic_processor 导入失败: {e}")

    try:
        # 测试导出模块
        from export.data_export import DataExporter
        print("✅ export.data_export 导入成功")
    except ImportError as e:
        print(f"❌ export.data_export 导入失败: {e}")

    try:
        # 测试分析模块
        from analysis.anomaly_detection import AnomalyDetector
        print("✅ analysis.anomaly_detection 导入成功")
    except ImportError as e:
        print(f"❌ analysis.anomaly_detection 导入失败: {e}")

    try:
        # 测试工具模块
        from utils.stock_info import StockInfo
        print("✅ utils.stock_info 导入成功")
    except ImportError as e:
        print(f"❌ utils.stock_info 导入失败: {e}")

    print("\n🎉 包结构测试完成！")
    print("如果看到上述✅标记，说明代码重构成功。")
    print("如果有❌标记，通常是因为缺少第三方依赖包。")

if __name__ == '__main__':
    test_imports()
