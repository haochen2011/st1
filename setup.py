#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
系统安装和初始化脚本
"""

import os
import sys
import subprocess
import pymysql
from pathlib import Path


def install_dependencies():
    """安装Python依赖包"""
    print("正在安装Python依赖包...")
    try:
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', '-r', 'requirements.txt','-i','https://pypi.tuna.tsinghua.edu.cn/simple'])
        print("✓ 依赖包安装成功")
        return True
    except subprocess.CalledProcessError as e:
        print(f"✗ 依赖包安装失败: {e}")
        return False


def create_database():
    """创建数据库"""
    print("正在创建数据库...")

    # 获取数据库配置
    host = input("请输入MySQL主机地址 (默认: localhost): ").strip() or "localhost"
    port = int(input("请输入MySQL端口 (默认: 3306): ").strip() or "3306")
    user = input("请输入MySQL用户名 (默认: root): ").strip() or "root"
    password = input("请输入MySQL密码: ").strip()
    database = input("请输入数据库名 (默认: stock_analysis): ").strip() or "stock_analysis"

    try:
        # 连接MySQL（不指定数据库）
        connection = pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            charset='utf8mb4'
        )

        cursor = connection.cursor()

        # 创建数据库
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
        print(f"✓ 数据库 {database} 创建成功")

        cursor.close()
        connection.close()

        # 更新配置文件
        update_config(host, port, user, password, database)

        return True

    except Exception as e:
        print(f"✗ 数据库创建失败: {e}")
        return False


def update_config(host, port, user, password, database):
    """更新配置文件"""
    config_content = f"""[database]
host = {host}
port = {port}
user = {user}
password = {password}
database = {database}

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
        f.write(config_content)

    print("✓ 配置文件更新成功")


def create_directories():
    """创建必要的目录"""
    print("正在创建目录结构...")

    directories = [
        'data/tick',
        'data/basic',
        'data/indicator',
        'logs'
    ]

    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)
        print(f"✓ 创建目录: {directory}")


def initialize_database():
    """初始化数据库表"""
    print("正在初始化数据库表...")

    try:
        # 导入数据库模块
        from data.database import db_manager

        # 数据库表会在DatabaseManager初始化时自动创建
        print("✓ 数据库表初始化成功")
        return True

    except Exception as e:
        print(f"✗ 数据库表初始化失败: {e}")
        return False


def run_tests():
    """运行系统测试"""
    print("正在运行系统测试...")

    try:
        subprocess.check_call([sys.executable, 'test_system.py'])
        return True
    except subprocess.CalledProcessError as e:
        print(f"✗ 系统测试失败: {e}")
        return False


def main():
    """主安装函数"""
    print("=" * 60)
    print("股票数据分析系统 - 安装向导")
    print("=" * 60)

    steps = [
        ("创建目录结构", create_directories),
        ("安装Python依赖", install_dependencies),
        ("创建数据库", create_database),
        ("初始化数据库表", initialize_database),
        ("运行系统测试", run_tests)
    ]

    success_count = 0

    for step_name, step_func in steps:
        print(f"\n步骤: {step_name}")
        print("-" * 40)

        if step_func():
            success_count += 1
            print(f"✓ {step_name} 完成")
        else:
            print(f"✗ {step_name} 失败")

            # 询问是否继续
            if input("是否继续下一步？(y/N): ").lower() != 'y':
                break

    print("\n" + "=" * 60)
    print("安装摘要")
    print("=" * 60)
    print(f"完成步骤: {success_count}/{len(steps)}")

    if success_count == len(steps):
        print("\n🎉 系统安装成功！")
        print("\n下一步操作:")
        print("1. 运行服务器: python run_server.py")
        print("2. 访问API: http://localhost:5000/api/health")
        print("3. 查看文档: 阅读 README.md")
    else:
        print("\n⚠️ 系统安装未完全成功，请检查错误信息并重新运行安装。")


if __name__ == '__main__':
    try:
        import traceback
        from processors.basic_processor import basic_processor
    except ImportError as e:
        print(f"导入basic_processor模块失败: {e}")
        traceback.print_exc()  # 打印完整的错误堆栈
    #main()