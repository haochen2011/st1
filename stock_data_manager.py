#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票数据管理器 - 主控制台
提供一键操作的命令行界面，整合所有功能
优化版本，支持批量处理、自动化和监控
"""

import sys
import os
import argparse
import time
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Union
import json
import schedule
import threading
from loguru import logger

# 添加当前目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# 导入自定义模块
from batch_processor import batch_processor
from enhanced_database import enhanced_db_manager
from enhanced_excel_exporter import enhanced_excel_exporter
from config import config
from stock_info import stock_info
from tick_data import tick_data
from basic_data import basic_data


class StockDataManager:
    """股票数据管理器主类"""

    def __init__(self):
        self.batch_processor = batch_processor
        self.db_manager = enhanced_db_manager
        self.excel_exporter = enhanced_excel_exporter

        # 设置日志
        self.setup_logging()

        # 运行状态
        self.is_running = False
        self.scheduler_thread = None

    def setup_logging(self):
        """设置日志配置"""
        try:
            log_dir = Path('./logs')
            log_dir.mkdir(exist_ok=True)

            log_file = log_dir / f"stock_manager_{datetime.now().strftime('%Y%m%d')}.log"

            # 配置loguru
            logger.add(
                log_file,
                rotation="1 day",
                retention="30 days",
                level="INFO",
                format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} - {message}",
                encoding="utf-8"
            )

            logger.info("股票数据管理器启动")

        except Exception as e:
            print(f"设置日志失败: {e}")

    def show_menu(self):
        """显示主菜单"""
        print("\n" + "=" * 60)
        print("          A股数据管理系统 v2.0 (优化版)")
        print("=" * 60)
        print("1.  📊 一键更新所有A股数据")
        print("2.  📈 批量更新股票基本信息")
        print("3.  ⏰ 批量下载分笔数据")
        print("4.  📉 批量下载K线数据")
        print("5.  📋 导出所有数据到Excel")
        print("6.  🔍 查询单个股票详情")
        print("7.  📊 查看数据库统计")
        print("8.  🔧 数据库维护工具")
        print("9.  ⏰ 设置定时任务")
        print("10. 📱 启动Web API服务")
        print("11. 🧹 数据清理工具")
        print("12. 📈 技术指标批量计算")
        print("0.  🚪 退出系统")
        print("=" * 60)

    def run_interactive(self):
        """运行交互式界面"""
        while True:
            try:
                self.show_menu()
                choice = input("\n请选择操作 (0-12): ").strip()

                if choice == '0':
                    print("感谢使用，再见！")
                    break
                elif choice == '1':
                    self.one_click_update_all()
                elif choice == '2':
                    self.batch_update_stock_info()
                elif choice == '3':
                    self.batch_download_tick_data()
                elif choice == '4':
                    self.batch_download_kline_data()
                elif choice == '5':
                    self.export_all_to_excel()
                elif choice == '6':
                    self.query_single_stock()
                elif choice == '7':
                    self.show_database_stats()
                elif choice == '8':
                    self.database_maintenance()
                elif choice == '9':
                    self.setup_scheduled_tasks()
                elif choice == '10':
                    self.start_web_api()
                elif choice == '11':
                    self.data_cleanup_tools()
                elif choice == '12':
                    self.batch_calculate_indicators()
                else:
                    print("无效选择，请重新输入")

                input("\n按回车键继续...")

            except KeyboardInterrupt:
                print("\n\n用户中断操作，退出系统")
                break
            except Exception as e:
                logger.error(f"操作异常: {e}")
                print(f"操作失败: {e}")
                input("按回车键继续...")

    def one_click_update_all(self):
        """一键更新所有数据"""
        print("\n🚀 开始一键更新所有A股数据...")
        print("这可能需要较长时间，请耐心等待...")

        # 获取用户设置
        include_tick = input("是否包含分笔数据? (y/N): ").strip().lower() == 'y'
        include_basic = input("是否包含K线数据? (Y/n): ").strip().lower() != 'n'

        periods = ['daily']
        if include_basic:
            period_choice = input("选择K线周期 (1=仅日线, 2=包含小时线, 3=全部): ").strip()
            if period_choice == '2':
                periods = ['daily', '1hour']
            elif period_choice == '3':
                periods = ['daily', '1hour', '30min', '15min', '5min']

        trade_date = None
        if include_tick:
            date_input = input("分笔数据日期 (YYYYMMDD, 回车=昨天): ").strip()
            if date_input:
                trade_date = date_input

        start_time = time.time()

        try:
            results = self.batch_processor.one_click_update_all(
                include_tick=include_tick,
                include_basic=include_basic,
                periods=periods,
                trade_date=trade_date
            )

            elapsed_time = time.time() - start_time

            print("\n✅ 一键更新完成!")
            print(f"⏱️  总耗时: {elapsed_time:.2f} 秒")
            print(
                f"📊 股票信息: 成功 {results['stock_info'].get('success', 0)}, 失败 {results['stock_info'].get('failed', 0)}")

            if include_tick:
                print(
                    f"⏰ 分笔数据: 成功 {results['tick_data'].get('success', 0)}, 失败 {results['tick_data'].get('failed', 0)}")

            if include_basic:
                print(
                    f"📈 K线数据: 成功 {results['basic_data'].get('success', 0)}, 失败 {results['basic_data'].get('failed', 0)}")

            # 询问是否导出Excel
            if input("\n是否导出数据到Excel? (y/N): ").strip().lower() == 'y':
                self.export_all_to_excel()

        except Exception as e:
            logger.error(f"一键更新失败: {e}")
            print(f"❌ 一键更新失败: {e}")

    def batch_update_stock_info(self):
        """批量更新股票基本信息"""
        print("\n📈 批量更新股票基本信息...")

        try:
            results = self.batch_processor.batch_update_stock_info()

            print(f"✅ 更新完成: 成功 {results['success']}, 失败 {results['failed']}")

            if results['errors']:
                print(f"❌ 错误详情 (前10个):")
                for error in results['errors'][:10]:
                    print(f"   - {error}")

        except Exception as e:
            logger.error(f"批量更新股票信息失败: {e}")
            print(f"❌ 批量更新失败: {e}")

    def batch_download_tick_data(self):
        """批量下载分笔数据"""
        print("\n⏰ 批量下载分笔数据...")

        date_input = input("请输入交易日期 (YYYYMMDD, 回车=昨天): ").strip()
        if not date_input:
            trade_date = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
        else:
            trade_date = date_input

        print(f"开始下载 {trade_date} 的分笔数据...")

        try:
            results = self.batch_processor.batch_download_tick_data(trade_date)

            print(f"✅ 下载完成: 成功 {results['success']}, 失败 {results['failed']}")

            if results['errors']:
                print(f"❌ 错误详情 (前10个):")
                for error in results['errors'][:10]:
                    print(f"   - {error}")

        except Exception as e:
            logger.error(f"批量下载分笔数据失败: {e}")
            print(f"❌ 批量下载失败: {e}")

    def batch_download_kline_data(self):
        """批量下载K线数据"""
        print("\n📉 批量下载K线数据...")

        print("选择周期:")
        print("1. 仅日线")
        print("2. 日线 + 小时线")
        print("3. 全部周期 (日线, 小时线, 30分钟, 15分钟, 5分钟)")

        choice = input("请选择 (1-3): ").strip()

        if choice == '1':
            periods = ['daily']
        elif choice == '2':
            periods = ['daily', '1hour']
        elif choice == '3':
            periods = ['daily', '1hour', '30min', '15min', '5min']
        else:
            print("无效选择，使用默认日线")
            periods = ['daily']

        start_date = input("开始日期 (YYYY-MM-DD, 回车=最近30天): ").strip()
        if not start_date:
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')

        print(f"开始下载周期: {periods}, 起始日期: {start_date}")

        try:
            results = self.batch_processor.batch_download_basic_data(
                periods=periods,
                start_date=start_date
            )

            print(f"✅ 下载完成: 成功 {results['success']}, 失败 {results['failed']}")

            if results['errors']:
                print(f"❌ 错误详情 (前10个):")
                for error in results['errors'][:10]:
                    print(f"   - {error}")

        except Exception as e:
            logger.error(f"批量下载K线数据失败: {e}")
            print(f"❌ 批量下载失败: {e}")

    def export_all_to_excel(self):
        """导出所有数据到Excel"""
        print("\n📋 导出所有数据到Excel...")

        include_tick = input("是否包含分笔数据样本? (y/N): ").strip().lower() == 'y'

        try:
            filename = self.excel_exporter.export_all_stock_data(
                include_basic_data=True,
                include_tick_data=include_tick,
                include_indicators=True,
                recent_days=30
            )

            print(f"✅ 导出完成: {filename}")

            # 询问是否打开文件
            if input("是否打开Excel文件? (y/N): ").strip().lower() == 'y':
                os.system(f'start excel "{filename}"')  # Windows
                # os.system(f'open "{filename}"')  # macOS
                # os.system(f'xdg-open "{filename}"')  # Linux

        except Exception as e:
            logger.error(f"导出Excel失败: {e}")
            print(f"❌ 导出失败: {e}")

    def query_single_stock(self):
        """查询单个股票详情"""
        print("\n🔍 查询单个股票详情...")

        stock_code = input("请输入股票代码 (如: 000001): ").strip()
        if not stock_code:
            print("股票代码不能为空")
            return

        try:
            # 查询股票基本信息
            info_sql = "SELECT * FROM stock_info WHERE stock_code = :stock_code"
            stock_info_df = self.db_manager.query_to_dataframe(info_sql, {'stock_code': stock_code})

            if stock_info_df.empty:
                print(f"❌ 未找到股票代码: {stock_code}")
                return

            stock_name = stock_info_df.iloc[0]['stock_name']
            print(f"\n📊 股票信息: {stock_code} - {stock_name}")
            print("-" * 40)

            for col in stock_info_df.columns:
                value = stock_info_df.iloc[0][col]
                print(f"{col}: {value}")

            # 查询最新交易数据
            latest_sql = """
            SELECT * FROM basic_data 
            WHERE stock_code = :stock_code AND period = 'daily'
            ORDER BY trade_date DESC LIMIT 5
            """
            latest_data = self.db_manager.query_to_dataframe(latest_sql, {'stock_code': stock_code})

            if not latest_data.empty:
                print(f"\n📈 最新交易数据 (前5天):")
                print("-" * 40)
                print(latest_data.to_string(index=False))

            # 询问是否导出详细数据
            if input("\n是否导出该股票的详细数据到Excel? (y/N): ").strip().lower() == 'y':
                days = int(input("导出最近多少天的数据? (默认90): ").strip() or "90")
                filename = self.excel_exporter.export_stock_detail_by_code(stock_code, days)
                print(f"✅ 详细数据已导出: {filename}")

        except Exception as e:
            logger.error(f"查询股票详情失败: {e}")
            print(f"❌ 查询失败: {e}")

    def show_database_stats(self):
        """显示数据库统计"""
        print("\n📊 数据库统计信息...")

        try:
            # 股票基本信息统计
            stock_info_stats = self.db_manager.get_table_info('stock_info')
            print(f"\n📈 股票基本信息:")
            print(f"   总股票数: {stock_info_stats.get('total_rows', 0)}")

            # 基础数据统计
            basic_data_stats = self.db_manager.get_table_info('basic_data')
            print(f"\n📉 基础数据:")
            print(f"   总记录数: {basic_data_stats.get('total_rows', 0)}")
            print(f"   最早数据: {basic_data_stats.get('earliest_data', 'N/A')}")
            print(f"   最新数据: {basic_data_stats.get('latest_data', 'N/A')}")

            # 分笔数据统计
            tick_data_stats = self.db_manager.get_table_info('tick_data')
            print(f"\n⏰ 分笔数据:")
            print(f"   总记录数: {tick_data_stats.get('total_rows', 0)}")
            print(f"   最早数据: {tick_data_stats.get('earliest_data', 'N/A')}")
            print(f"   最新数据: {tick_data_stats.get('latest_data', 'N/A')}")

            # 详细统计
            detail_sql = """
            SELECT 
                (SELECT COUNT(DISTINCT stock_code) FROM basic_data WHERE period = 'daily') as stocks_with_daily,
                (SELECT COUNT(DISTINCT stock_code) FROM tick_data) as stocks_with_tick,
                (SELECT COUNT(DISTINCT trade_date) FROM basic_data WHERE period = 'daily') as trading_days,
                (SELECT market, COUNT(*) as cnt FROM stock_info GROUP BY market ORDER BY cnt DESC LIMIT 1) as largest_market
            """
            detail_stats = self.db_manager.query_to_dataframe(detail_sql)

            if not detail_stats.empty:
                print(f"\n📊 详细统计:")
                print(f"   有日线数据的股票: {detail_stats.iloc[0]['stocks_with_daily']}")
                print(f"   有分笔数据的股票: {detail_stats.iloc[0]['stocks_with_tick']}")
                print(f"   交易日天数: {detail_stats.iloc[0]['trading_days']}")

        except Exception as e:
            logger.error(f"获取数据库统计失败: {e}")
            print(f"❌ 获取统计失败: {e}")

    def database_maintenance(self):
        """数据库维护工具"""
        print("\n🔧 数据库维护工具...")
        print("1. 优化所有表")
        print("2. 清理重复数据")
        print("3. 重建索引")
        print("4. 数据备份")

        choice = input("请选择维护操作 (1-4): ").strip()

        try:
            if choice == '1':
                print("开始优化表...")
                tables = ['stock_info', 'basic_data', 'tick_data', 'indicator_data']
                for table in tables:
                    print(f"优化表: {table}")
                    self.db_manager.optimize_table(table)
                print("✅ 表优化完成")

            elif choice == '2':
                print("⚠️  清理重复数据功能需要谨慎操作，建议先备份数据")
                confirm = input("确认继续? (yes/no): ").strip().lower()
                if confirm == 'yes':
                    # 这里可以添加清理重复数据的逻辑
                    print("清理重复数据功能开发中...")

            elif choice == '3':
                print("重建索引功能开发中...")

            elif choice == '4':
                print("数据备份功能开发中...")

            else:
                print("无效选择")

        except Exception as e:
            logger.error(f"数据库维护失败: {e}")
            print(f"❌ 维护失败: {e}")

    def setup_scheduled_tasks(self):
        """设置定时任务"""
        print("\n⏰ 设置定时任务...")

        if self.is_running:
            print("定时任务已在运行中")
            if input("是否停止当前任务? (y/N): ").strip().lower() == 'y':
                self.stop_scheduler()
            else:
                return

        print("设置定时任务:")
        print("1. 每日16:00更新基础数据")
        print("2. 每日16:30更新分笔数据")
        print("3. 每周一09:00更新股票信息")
        print("4. 启动所有定时任务")

        choice = input("请选择 (1-4): ").strip()

        try:
            if choice in ['1', '4']:
                schedule.every().day.at("16:00").do(self._scheduled_basic_update)
                print("✅ 已设置每日基础数据更新")

            if choice in ['2', '4']:
                schedule.every().day.at("16:30").do(self._scheduled_tick_update)
                print("✅ 已设置每日分笔数据更新")

            if choice in ['3', '4']:
                schedule.every().monday.at("09:00").do(self._scheduled_stock_info_update)
                print("✅ 已设置每周股票信息更新")

            if choice in ['1', '2', '3', '4']:
                self.start_scheduler()
                print("✅ 定时任务已启动")
            else:
                print("无效选择")

        except Exception as e:
            logger.error(f"设置定时任务失败: {e}")
            print(f"❌ 设置失败: {e}")

    def start_scheduler(self):
        """启动定时任务调度器"""
        if not self.is_running:
            self.is_running = True
            self.scheduler_thread = threading.Thread(target=self._run_scheduler, daemon=True)
            self.scheduler_thread.start()
            logger.info("定时任务调度器已启动")

    def stop_scheduler(self):
        """停止定时任务调度器"""
        self.is_running = False
        schedule.clear()
        if self.scheduler_thread and self.scheduler_thread.is_alive():
            self.scheduler_thread.join(timeout=1)
        logger.info("定时任务调度器已停止")

    def _run_scheduler(self):
        """运行定时任务调度器"""
        while self.is_running:
            schedule.run_pending()
            time.sleep(60)  # 每分钟检查一次

    def _scheduled_basic_update(self):
        """定时基础数据更新"""
        logger.info("定时任务: 开始更新基础数据")
        self.batch_processor.batch_download_basic_data(['daily'])

    def _scheduled_tick_update(self):
        """定时分笔数据更新"""
        logger.info("定时任务: 开始更新分笔数据")
        today = datetime.now().strftime('%Y%m%d')
        self.batch_processor.batch_download_tick_data(today)

    def _scheduled_stock_info_update(self):
        """定时股票信息更新"""
        logger.info("定时任务: 开始更新股票信息")
        self.batch_processor.batch_update_stock_info()

    def start_web_api(self):
        """启动Web API服务"""
        print("\n📱 启动Web API服务...")
        try:
            from run_server import app
            print("Web API服务启动中...")
            print("访问地址: http://localhost:5000")
            app.run(host='0.0.0.0', port=5000, debug=False)
        except Exception as e:
            logger.error(f"启动Web API失败: {e}")
            print(f"❌ 启动失败: {e}")

    def data_cleanup_tools(self):
        """数据清理工具"""
        print("\n🧹 数据清理工具...")
        print("1. 清理超过1年的分笔数据")
        print("2. 清理无效的股票数据")
        print("3. 压缩历史数据")

        choice = input("请选择清理操作 (1-3): ").strip()

        try:
            if choice == '1':
                days = int(input("保留最近多少天的分笔数据? (默认365): ").strip() or "365")
                confirm = input(f"确认清理超过{days}天的分笔数据? (yes/no): ").strip().lower()
                if confirm == 'yes':
                    result = self.db_manager.cleanup_old_data('tick_data', 'trade_date', days)
                    if result:
                        print("✅ 分笔数据清理完成")

            elif choice == '2':
                print("清理无效股票数据功能开发中...")

            elif choice == '3':
                print("压缩历史数据功能开发中...")

            else:
                print("无效选择")

        except Exception as e:
            logger.error(f"数据清理失败: {e}")
            print(f"❌ 清理失败: {e}")

    def batch_calculate_indicators(self):
        """批量计算技术指标"""
        print("\n📈 批量计算技术指标...")
        print("功能开发中，敬请期待...")

    def run_cli(self, args):
        """命令行模式运行"""
        if args.command == 'update-all':
            self.batch_processor.one_click_update_all(
                include_tick=args.include_tick,
                include_basic=args.include_basic,
                trade_date=args.date
            )
        elif args.command == 'update-stocks':
            self.batch_processor.batch_update_stock_info()
        elif args.command == 'download-tick':
            self.batch_processor.batch_download_tick_data(args.date)
        elif args.command == 'download-basic':
            periods = args.periods.split(',') if args.periods else ['daily']
            self.batch_processor.batch_download_basic_data(periods)
        elif args.command == 'export-excel':
            self.excel_exporter.export_all_stock_data()
        elif args.command == 'stats':
            self.show_database_stats()
        else:
            print(f"未知命令: {args.command}")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='A股数据管理系统')
    parser.add_argument('--mode', choices=['interactive', 'cli'], default='interactive',
                        help='运行模式: interactive(交互式) 或 cli(命令行)')

    # CLI模式参数
    parser.add_argument('--command', choices=[
        'update-all', 'update-stocks', 'download-tick', 'download-basic',
        'export-excel', 'stats'
    ], help='命令行模式的命令')

    parser.add_argument('--date', help='日期参数 (YYYYMMDD格式)')
    parser.add_argument('--periods', help='周期参数 (逗号分隔)')
    parser.add_argument('--include-tick', action='store_true', help='包含分笔数据')
    parser.add_argument('--include-basic', action='store_true', help='包含基础数据')

    args = parser.parse_args()

    # 创建管理器实例
    manager = StockDataManager()

    try:
        if args.mode == 'interactive' or not args.command:
            # 交互式模式
            manager.run_interactive()
        else:
            # 命令行模式
            manager.run_cli(args)

    except KeyboardInterrupt:
        print("\n程序被用户中断")
    except Exception as e:
        logger.error(f"程序运行异常: {e}")
        print(f"程序异常: {e}")
    finally:
        # 清理资源
        manager.stop_scheduler()
        manager.db_manager.close()


if __name__ == '__main__':
    main()