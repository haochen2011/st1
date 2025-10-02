#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
WebSocket实时推送服务器启动脚本
"""

import asyncio
import sys
import os
import signal
from loguru import logger

# 添加当前目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config import config
from realtime_push import realtime_server


class WebSocketServerManager:
    """WebSocket服务器管理器"""

    def __init__(self):
        self.host = config.get('websocket', 'host', '0.0.0.0')
        self.port = config.getint('websocket', 'port', 8765)
        self.server = None
        self.is_running = False

    async def start(self):
        """启动WebSocket服务器"""
        try:
            # 更新服务器配置
            realtime_server.host = self.host
            realtime_server.port = self.port

            logger.info(f"正在启动WebSocket服务器: ws://{self.host}:{self.port}")

            # 注册信号处理器
            self._setup_signal_handlers()

            # 启动服务器
            self.is_running = True
            await realtime_server.start_server()

        except KeyboardInterrupt:
            logger.info("收到中断信号，正在关闭服务器...")
        except Exception as e:
            logger.error(f"WebSocket服务器启动失败: {e}")
            raise
        finally:
            await self.stop()

    async def stop(self):
        """停止WebSocket服务器"""
        if self.is_running:
            logger.info("正在停止WebSocket服务器...")
            await realtime_server.stop_server()
            self.is_running = False
            logger.info("WebSocket服务器已停止")

    def _setup_signal_handlers(self):
        """设置信号处理器"""

        def signal_handler(signum, frame):
            logger.info(f"收到信号 {signum}，准备关闭服务器...")
            asyncio.create_task(self.stop())

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    def get_server_info(self):
        """获取服务器信息"""
        stats = realtime_server.pusher.get_statistics()
        return {
            'host': self.host,
            'port': self.port,
            'is_running': self.is_running,
            'statistics': stats
        }


async def main():
    """主函数"""
    logger.info("股票数据分析系统 - WebSocket实时推送服务器")

    # 配置日志
    log_file = config.get('logging', 'file', './logs/websocket_server.log')
    log_level = config.get('logging', 'level', 'INFO')

    logger.add(
        log_file,
        rotation="1 day",
        retention="30 days",
        level=log_level,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {module}:{function}:{line} | {message}"
    )

    # 创建服务器管理器
    server_manager = WebSocketServerManager()

    try:
        # 启动服务器
        await server_manager.start()
    except Exception as e:
        logger.error(f"服务器运行失败: {e}")
        sys.exit(1)


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("用户中断程序")
    except Exception as e:
        logger.error(f"程序异常退出: {e}")
        sys.exit(1)