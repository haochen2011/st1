#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
服务器启动脚本
启动Flask API服务器
"""

import os
import sys
from pathlib import Path
from flask import Flask
from flask_cors import CORS
from loguru import logger

# 添加当前目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config import config
from indicator_api import indicator_api
from export_api import export_api

def create_app():
    """创建Flask应用"""
    app = Flask(__name__)

    # 配置CORS
    CORS(app)

    # 注册蓝图
    app.register_blueprint(indicator_api)
    app.register_blueprint(export_api)
    # 配置日志
    log_file = config.get('logging', 'file', './logs/stock_analysis.log')
    log_level = config.get('logging', 'level', 'INFO')

    # 创建日志目录
    Path(os.path.dirname(log_file)).mkdir(parents=True, exist_ok=True)

    # 配置loguru
    logger.add(
        log_file,
        rotation="1 day",
        retention="30 days",
        level=log_level,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {module}:{function}:{line} | {message}"
    )

    logger.info("股票分析系统启动")

    return app


def main():
    """主函数"""
    try:
        # 创建必要的目录
        for path_type in ['tick_data', 'basic_data', 'indicator_data']:
            config.get_data_path(path_type)

        # 创建Flask应用
        app = create_app()

        # 获取配置
        host = config.get('api', 'host', '0.0.0.0')
        port = config.getint('api', 'port', 5000)
        debug = config.getboolean('api', 'debug', False)

        logger.info(f"启动服务器: {host}:{port}, 调试模式: {debug}")

        # 启动服务器
        app.run(host=host, port=port, debug=debug)

    except KeyboardInterrupt:
        logger.info("服务器被用户中断")
    except Exception as e:
        logger.error(f"服务器启动失败: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()