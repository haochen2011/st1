#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
应用所有代码修复的脚本

使用方法：
    python apply_fixes.py

或者在代码中导入：
    from apply_fixes import apply_all_fixes
    apply_all_fixes()
"""

import sys
from pathlib import Path
from loguru import logger

# 确保能导入项目模块
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))


def apply_stock_info_fixes():
    """应用stock_info模块的修复"""
    try:
        from utils.stock_info import StockInfo
        from fix_stock_info import get_stock_list_fixed, update_stock_info_to_db_fixed
        
        # 替换方法（猴子补丁）
        StockInfo.get_stock_list = get_stock_list_fixed
        StockInfo.update_stock_info_to_db = update_stock_info_to_db_fixed
        
        logger.success("✓ stock_info 修复已应用")
        return True
        
    except Exception as e:
        logger.error(f"✗ stock_info 修复失败: {e}")
        return False


def apply_limit_up_analysis_fixes():
    """应用涨停分析模块的修复"""
    try:
        from analysis.limit_up_analysis import LimitUpAnalyzer
        from fix_limit_up_analysis import (
            generate_limit_up_report_fixed,
            _get_limit_up_type
        )
        
        # 替换方法
        LimitUpAnalyzer.generate_limit_up_report = generate_limit_up_report_fixed
        LimitUpAnalyzer._get_limit_up_type = _get_limit_up_type
        
        logger.success("✓ limit_up_analysis 修复已应用")
        return True
        
    except Exception as e:
        logger.error(f"✗ limit_up_analysis 修复失败: {e}")
        return False


def apply_data_export_fixes():
    """应用数据导出模块的修复"""
    try:
        from export.data_export import DataExporter
        from fix_data_export import (
            _export_dataframe_enhanced,
            _clean_dataframe_for_export,
            _export_to_excel,
            _export_to_csv,
            _export_to_json,
            _export_to_parquet,
            _export_to_html,
            export_with_validation
        )
        
        # 替换方法
        DataExporter._export_dataframe = _export_dataframe_enhanced
        DataExporter._clean_dataframe_for_export = _clean_dataframe_for_export
        DataExporter._export_to_excel = _export_to_excel
        DataExporter._export_to_csv = _export_to_csv
        DataExporter._export_to_json = _export_to_json
        DataExporter._export_to_parquet = _export_to_parquet
        DataExporter._export_to_html = _export_to_html
        DataExporter.export_with_validation = export_with_validation
        
        logger.success("✓ data_export 修复已应用")
        return True
        
    except Exception as e:
        logger.error(f"✗ data_export 修复失败: {e}")
        return False


def apply_rate_limiter():
    """应用API限流器到相关模块"""
    try:
        from rate_limiter import rate_limiter
        from utils.stock_info import StockInfo
        from data.data_fetcher import DataFetcher
        
        # 为关键API调用方法添加限流
        if hasattr(StockInfo, 'get_stock_basic_info'):
            original_method = StockInfo.get_stock_basic_info
            StockInfo.get_stock_basic_info = rate_limiter(original_method)
            logger.debug("  - StockInfo.get_stock_basic_info 已添加限流")
        
        if hasattr(DataFetcher, 'get_realtime_data'):
            original_method = DataFetcher.get_realtime_data
            DataFetcher.get_realtime_data = rate_limiter(original_method)
            logger.debug("  - DataFetcher.get_realtime_data 已添加限流")
        
        if hasattr(DataFetcher, 'get_historical_data'):
            original_method = DataFetcher.get_historical_data
            DataFetcher.get_historical_data = rate_limiter(original_method)
            logger.debug("  - DataFetcher.get_historical_data 已添加限流")
        
        logger.success("✓ rate_limiter 已应用到关键API")
        return True
        
    except Exception as e:
        logger.error(f"✗ rate_limiter 应用失败: {e}")
        return False


def apply_all_fixes():
    """应用所有修复"""
    logger.info("=" * 60)
    logger.info("开始应用代码修复...")
    logger.info("=" * 60)
    
    results = {
        'stock_info': apply_stock_info_fixes(),
        'limit_up_analysis': apply_limit_up_analysis_fixes(),
        'data_export': apply_data_export_fixes(),
        'rate_limiter': apply_rate_limiter(),
    }
    
    logger.info("=" * 60)
    logger.info("修复应用完成")
    logger.info("=" * 60)
    
    # 统计结果
    success_count = sum(1 for v in results.values() if v)
    total_count = len(results)
    
    logger.info(f"\n结果: {success_count}/{total_count} 个模块修复成功")
    
    if success_count == total_count:
        logger.success("\n✓ 所有修复已成功应用！")
    else:
        logger.warning(f"\n⚠ 部分修复失败，请检查错误信息")
        for module, success in results.items():
            status = "✓" if success else "✗"
            logger.info(f"  {status} {module}")
    
    return results


def test_fixes():
    """测试修复是否生效"""
    logger.info("\n" + "=" * 60)
    logger.info("测试修复效果...")
    logger.info("=" * 60)
    
    test_results = []
    
    # 测试1：stock_info 获取所有A股
    try:
        from utils.stock_info import stock_info
        
        logger.info("\n测试1: 获取所有A股股票列表...")
        stocks = stock_info.get_stock_list('all')
        
        if not stocks.empty:
            sh_count = len(stocks[stocks['market'] == 'sh'])
            sz_count = len(stocks[stocks['market'] == 'sz'])
            bj_count = len(stocks[stocks['market'] == 'bj'])
            
            logger.info(f"  总计: {len(stocks)} 只")
            logger.info(f"  上海: {sh_count} 只")
            logger.info(f"  深圳: {sz_count} 只")
            logger.info(f"  北交所: {bj_count} 只")
            
            if bj_count > 0:
                logger.success("  ✓ 测试通过：成功获取北交所股票")
                test_results.append(True)
            else:
                logger.warning("  ⚠ 未获取到北交所股票")
                test_results.append(False)
        else:
            logger.error("  ✗ 获取股票列表失败")
            test_results.append(False)
            
    except Exception as e:
        logger.error(f"  ✗ 测试失败: {e}")
        test_results.append(False)
    
    # 测试2：限流器
    try:
        from rate_limiter import rate_limiter
        import time
        
        logger.info("\n测试2: API限流器...")
        
        @rate_limiter
        def test_api_call(i):
            return f"调用{i}"
        
        start_time = time.time()
        for i in range(15):  # 调用15次，应该会触发限流
            test_api_call(i)
        elapsed = time.time() - start_time
        
        # 15次调用，每10次暂停1秒，应该至少花费1秒
        if elapsed >= 1.0:
            logger.success(f"  ✓ 测试通过：限流器工作正常 (耗时: {elapsed:.2f}秒)")
            test_results.append(True)
        else:
            logger.warning(f"  ⚠ 限流可能未生效 (耗时: {elapsed:.2f}秒)")
            test_results.append(False)
            
    except Exception as e:
        logger.error(f"  ✗ 测试失败: {e}")
        test_results.append(False)
    
    # 测试3：数据导出
    try:
        from export.data_export import data_exporter
        import pandas as pd
        
        logger.info("\n测试3: 数据导出功能...")
        
        test_df = pd.DataFrame({
            'stock_code': ['600000', '000001'],
            'stock_name': ['浦发银行', '平安银行'],
            'price': [10.5, 15.8],
            'change_pct': [2.3, -1.5]
        })
        
        filepath = data_exporter._export_dataframe(test_df, 'test_export', 'csv')
        
        if filepath and Path(filepath).exists():
            logger.success(f"  ✓ 测试通过：数据导出成功 ({filepath})")
            # 清理测试文件
            Path(filepath).unlink()
            test_results.append(True)
        else:
            logger.error("  ✗ 数据导出失败")
            test_results.append(False)
            
    except Exception as e:
        logger.error(f"  ✗ 测试失败: {e}")
        test_results.append(False)
    
    # 汇总测试结果
    logger.info("\n" + "=" * 60)
    success_count = sum(test_results)
    total_count = len(test_results)
    logger.info(f"测试结果: {success_count}/{total_count} 通过")
    logger.info("=" * 60)
    
    return all(test_results)


if __name__ == '__main__':
    # 应用修复
    fix_results = apply_all_fixes()
    
    # 运行测试
    if all(fix_results.values()):
        logger.info("\n是否运行测试? (建议运行)")
        test_fixes()
    
    logger.info("\n" + "=" * 60)
    logger.info("修复应用完成！")
    logger.info("=" * 60)
    logger.info("\n使用说明：")
    logger.info("1. 在主程序开始时调用 apply_all_fixes()")
    logger.info("2. 或在启动脚本中导入: from apply_fixes import apply_all_fixes")
    logger.info("3. 所有修复会自动应用到相关模块")
    logger.info("\n关键改进：")
    logger.info("✓ stock_info 现在获取所有A股（包括北交所）")
    logger.info("✓ 涨停分析使用实时行情数据")
    logger.info("✓ API调用每10次自动暂停1秒")
    logger.info("✓ 数据导出增强错误处理和大文件支持")
