#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
A股数据管理系统 - 一键启动脚本
提供最常用功能的快速访问
"""

import sys
import os
from datetime import datetime
from pathlib import Path

# 添加当前目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# 新增：打印Python搜索路径，检查是否包含目标目录
print("当前Python搜索路径:")
for path in sys.path:
    print(f"  - {path}")
def check_environment():
    """检查运行环境"""
    print("🔍 检查运行环境...")

    # 检查Python版本
    if sys.version_info < (3, 8):
        print("❌ Python版本过低，需要3.8以上版本")
        return False

    # 检查必要的包
    required_packages = [
        'pandas', 'numpy', 'akshare', 'sqlalchemy',
        'pymysql', 'openpyxl', 'loguru', 'tqdm'
    ]

    missing_packages = []
    for package in required_packages:
        try:
            __import__(package)
        except ImportError:
            missing_packages.append(package)

    if missing_packages:
        print(f"❌ 缺少以下依赖包: {', '.join(missing_packages)}")
        print("请运行: pip install -r requirements.txt")
        return False

    # 检查配置文件
    if not os.path.exists('config.ini'):
        print("⚠️  配置文件 config.ini 不存在，将创建默认配置")
        create_default_config()

    print("✅ 环境检查通过")
    return True


def create_default_config():
    """创建默认配置文件"""
    default_config = """[database]
host = localhost
port = 3306
user = root
password = root
database = stock_dragon

[data_path]
tick_data = ./data/tick
basic_data = ./data/basic
indicator_data = ./data/indicator

[api]
host = 0.0.0.0
port = 5000
debug = True

[logging]
level = INFO
file = ./logs/stock_analysis.log

[stock]
market_codes = sh,sz
default_period = daily
periods = 1min,5min,10min,15min,30min,1hour,daily,week,month,quarter,half-year,year
"""

    with open('config.ini', 'w', encoding='utf-8') as f:
        f.write(default_config)

    print("✅ 默认配置文件已创建，请根据需要修改 config.ini")


def show_welcome():
    """显示欢迎信息"""
    print("\n" + "=" * 60)
    print("🚀 A股数据管理系统 v2.0 - 一键启动")
    print("=" * 60)
    print("📊 支持功能:")
    print("   • 一键获取所有A股数据（4000+只股票）")
    print("   • 分笔数据、K线数据批量下载")
    print("   • 智能Excel导出，多表格格式化")
    print("   • 定时任务，自动化数据更新")
    print("   • 数据库优化，高性能存储")
    print("=" * 60)


def quick_menu():
    """快速菜单"""
    print("\n🎯 快速操作菜单:")
    print("1. 🚀 一键更新所有A股数据 (推荐新用户)")
    print("2. 📈 只更新股票基本信息")
    print("3. ⏰ 下载昨日分笔数据")
    print("4. 📊 导出所有数据到Excel")
    print("5. 🔍 查询单个股票")
    print("6. 📱 启动完整管理界面")
    print("7. ⚙️  系统配置和帮助")
    print("0. 🚪 退出")
    print("-" * 60)


def one_click_update():
    """一键更新所有数据"""
    print("\n🚀 开始一键更新所有A股数据...")
    print("这是最全面的数据更新，包含:")
    print("   • 股票基本信息 (代码、名称、行业等)")
    print("   • 最新分笔数据 (逐笔成交明细)")
    print("   • K线数据 (日线、小时线等)")
    print()

    # 询问用户选择
    print("请选择更新范围:")
    print("1. 基础更新 (股票信息 + 日线数据)")
    print("2. 标准更新 (基础 + 分笔数据)")
    print("3. 完整更新 (标准 + 多周期K线)")

    choice = input("请选择 (1-3, 默认2): ").strip() or "2"

    try:
        from processors.batch_processor import batch_processor

        if choice == "1":
            print("开始基础更新...")
            result = batch_processor.one_click_update_all(
                include_tick=False,
                include_basic=True,
                periods=['daily']
            )
        elif choice == "3":
            print("开始完整更新...")
            result = batch_processor.one_click_update_all(
                include_tick=True,
                include_basic=True,
                periods=['daily', '1hour', '30min', '15min']
            )
        else:  # choice == "2"
            print("开始标准更新...")
            result = batch_processor.one_click_update_all(
                include_tick=True,
                include_basic=True,
                periods=['daily']
            )

        print("\n✅ 更新完成!")
        print(
            f"📊 股票信息: 成功 {result['stock_info'].get('success', 0)}, 失败 {result['stock_info'].get('failed', 0)}")

        if 'tick_data' in result:
            print(
                f"⏰ 分笔数据: 成功 {result['tick_data'].get('success', 0)}, 失败 {result['tick_data'].get('failed', 0)}")

        if 'basic_data' in result:
            print(
                f"📈 K线数据: 成功 {result['basic_data'].get('success', 0)}, 失败 {result['basic_data'].get('failed', 0)}")

        print(f"⏱️  总耗时: {result.get('total_time', 0):.2f} 秒")

        # 询问是否导出Excel
        if input("\n是否立即导出数据到Excel? (y/N): ").strip().lower() == 'y':
            export_excel()

    except Exception as e:
        print(f"❌ 更新失败: {e}")
        print("💡 建议检查网络连接和数据库配置")


def update_stock_info():
    """更新股票基本信息"""
    print("\n📈 更新股票基本信息...")

    try:
        from processors.batch_processor import batch_processor

        result = batch_processor.batch_update_stock_info()
        print(f"✅ 更新完成: 成功 {result['success']}, 失败 {result['failed']}")

    except Exception as e:
        print(f"❌ 更新失败: {e}")


def download_tick_data():
    """下载分笔数据"""
    print("\n⏰ 下载分笔数据...")

    date_input = input("请输入日期 (YYYYMMDD, 回车=昨天): ").strip()

    try:
        from processors.batch_processor import batch_processor
        from datetime import datetime, timedelta

        if not date_input:
            trade_date = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
        else:
            trade_date = date_input

        print(f"开始下载 {trade_date} 的分笔数据...")
        result = batch_processor.batch_download_tick_data(trade_date)
        print(f"✅ 下载完成: 成功 {result['success']}, 失败 {result['failed']}")

    except Exception as e:
        print(f"❌ 下载失败: {e}")


def export_excel():
    """导出Excel"""
    print("\n📊 导出数据到Excel...")

    try:
        from export.enhanced_excel_exporter import enhanced_excel_exporter

        print("正在生成Excel文件...")
        filename = enhanced_excel_exporter.export_all_stock_data(
            include_basic_data=True,
            include_tick_data=True,
            include_indicators=True,
            recent_days=30
        )

        print(f"✅ 导出完成: {filename}")

        # 询问是否打开文件
        if input("是否打开Excel文件? (y/N): ").strip().lower() == 'y':
            try:
                import subprocess
                subprocess.run(['start', str(filename)], shell=True)  # Windows
            except:
                print(f"请手动打开文件: {filename}")

    except Exception as e:
        print(f"❌ 导出失败: {e}")


def query_stock():
    """查询单个股票"""
    print("\n🔍 查询单个股票...")

    stock_code = input("请输入股票代码 (如: 000001): ").strip()
    if not stock_code:
        print("股票代码不能为空")
        return

    try:
        from data.enhanced_database import enhanced_db_manager

        # 查询股票基本信息
        info_sql = "SELECT * FROM stock_info WHERE stock_code = :stock_code"
        stock_info = enhanced_db_manager.query_to_dataframe(info_sql, {'stock_code': stock_code})

        if stock_info.empty:
            print(f"❌ 未找到股票: {stock_code}")
            return

        stock_name = stock_info.iloc[0]['stock_name']
        print(f"\n📊 {stock_code} - {stock_name}")
        print(f"市场: {stock_info.iloc[0]['market']}")
        print(f"行业: {stock_info.iloc[0]['industry']}")
        print(f"上市日期: {stock_info.iloc[0]['list_date']}")

        # 查询最新价格
        price_sql = """
        SELECT trade_date, close_price, volume, amount
        FROM basic_data
        WHERE stock_code = :stock_code AND period = 'daily'
        ORDER BY trade_date DESC LIMIT 1
        """
        price_data = enhanced_db_manager.query_to_dataframe(price_sql, {'stock_code': stock_code})

        if not price_data.empty:
            latest = price_data.iloc[0]
            print(f"最新收盘价: {latest['close_price']:.2f}")
            print(f"最新交易日: {latest['trade_date']}")
            print(f"成交量: {latest['volume']:,}")
            print(f"成交额: {latest['amount']:,.2f}")

    except Exception as e:
        print(f"❌ 查询失败: {e}")


def show_help():
    """显示帮助信息"""
    print("\n⚙️  系统配置和帮助")
    print("=" * 50)
    print("📋 配置文件: config.ini")
    print("   请确保数据库连接信息正确")
    print()
    print("📁 目录结构:")
    print("   • logs/          - 日志文件")
    print("   • data/          - 数据文件")
    print("   • excel_exports/ - Excel导出文件")
    print("   • batch_output/  - 批量处理结果")
    print()
    print("🔧 常见问题:")
    print("   1. 数据库连接失败 -> 检查config.ini中的数据库配置")
    print("   2. 网络超时      -> 检查网络连接，建议使用稳定网络")
    print("   3. 内存不足      -> 减少并发线程数，分批处理")
    print()
    print("📞 技术支持:")
    print("   • 查看 logs/ 目录下的日志文件")
    print("   • 检查 README_优化版.md 详细文档")
    print()

    input("按回车键返回主菜单...")


def main():
    """主函数"""
    # 检查环境
    if not check_environment():
        input("按回车键退出...")
        return

    # 显示欢迎信息
    show_welcome()

    while True:
        try:
            quick_menu()
            choice = input("请选择操作 (0-7): ").strip()

            if choice == '0':
                print("👋 感谢使用，再见！")
                break
            elif choice == '1':
                one_click_update()
            elif choice == '2':
                update_stock_info()
            elif choice == '3':
                download_tick_data()
            elif choice == '4':
                export_excel()
            elif choice == '5':
                query_stock()
            elif choice == '6':
                print("\n🚀 启动完整管理界面...")
                from utils.stock_data_manager import StockDataManager
                manager = StockDataManager()
                manager.run_interactive()
                break
            elif choice == '7':
                show_help()
            else:
                print("❌ 无效选择，请重新输入")

            input("\n按回车键继续...")

        except KeyboardInterrupt:
            print("\n\n👋 用户中断，退出系统")
            break
        except Exception as e:
            print(f"\n❌ 操作异常: {e}")
            input("按回车键继续...")


if __name__ == '__main__':

    main()
