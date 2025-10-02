#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
实时数据推送模块
使用WebSocket实现实时股票数据推送
"""

import asyncio
import json
import time
import threading
from datetime import datetime, timedelta
from typing import Set, Dict, List
import websockets
import akshare as ak
from loguru import logger
from database import db_manager
from config import config
from basic_data import basic_data
from tick_data import tick_data


class RealtimePusher:
    """实时数据推送器"""

    def __init__(self):
        self.clients: Dict[websockets.WebSocketServerProtocol, Dict] = {}
        self.subscriptions: Dict[str, Set[websockets.WebSocketServerProtocol]] = {}
        self.is_running = False
        self.data_thread = None
        self.price_cache = {}  # 价格缓存
        self.update_interval = 3  # 更新间隔（秒）

    async def register_client(self, websocket, path):
        """注册新客户端"""
        try:
            self.clients[websocket] = {
                'id': id(websocket),
                'connected_at': datetime.now(),
                'subscriptions': set(),
                'last_ping': time.time()
            }

            logger.info(f"新客户端连接: {self.clients[websocket]['id']}")

            # 发送欢迎消息
            welcome_msg = {
                'type': 'welcome',
                'message': '欢迎连接股票数据推送服务',
                'timestamp': datetime.now().isoformat(),
                'client_id': self.clients[websocket]['id']
            }
            await websocket.send(json.dumps(welcome_msg, ensure_ascii=False))

            # 处理客户端消息
            await self.handle_client_messages(websocket)

        except websockets.exceptions.ConnectionClosed:
            logger.info(f"客户端断开连接: {self.clients.get(websocket, {}).get('id', 'unknown')}")
        except Exception as e:
            logger.error(f"处理客户端连接失败: {e}")
        finally:
            await self.unregister_client(websocket)

    async def unregister_client(self, websocket):
        """注销客户端"""
        if websocket in self.clients:
            client_info = self.clients[websocket]

            # 取消所有订阅
            for stock_code in list(client_info['subscriptions']):
                await self.unsubscribe_stock(websocket, stock_code)

            del self.clients[websocket]
            logger.info(f"客户端已注销: {client_info['id']}")

    async def handle_client_messages(self, websocket):
        """处理客户端消息"""
        async for message in websocket:
            try:
                data = json.loads(message)
                msg_type = data.get('type')

                if msg_type == 'subscribe':
                    await self.handle_subscribe(websocket, data)
                elif msg_type == 'unsubscribe':
                    await self.handle_unsubscribe(websocket, data)
                elif msg_type == 'ping':
                    await self.handle_ping(websocket, data)
                elif msg_type == 'get_subscriptions':
                    await self.handle_get_subscriptions(websocket)
                else:
                    await self.send_error(websocket, f"未知消息类型: {msg_type}")

            except json.JSONDecodeError:
                await self.send_error(websocket, "无效的JSON格式")
            except Exception as e:
                logger.error(f"处理客户端消息失败: {e}")
                await self.send_error(websocket, f"处理消息失败: {str(e)}")

    async def handle_subscribe(self, websocket, data):
        """处理订阅请求"""
        try:
            stock_code = data.get('stock_code')
            data_type = data.get('data_type', 'price')  # price, tick, basic

            if not stock_code:
                await self.send_error(websocket, "缺少股票代码")
                return

            await self.subscribe_stock(websocket, stock_code, data_type)

            response = {
                'type': 'subscribe_success',
                'stock_code': stock_code,
                'data_type': data_type,
                'timestamp': datetime.now().isoformat()
            }
            await websocket.send(json.dumps(response, ensure_ascii=False))

        except Exception as e:
            logger.error(f"处理订阅请求失败: {e}")
            await self.send_error(websocket, f"订阅失败: {str(e)}")

    async def handle_unsubscribe(self, websocket, data):
        """处理取消订阅请求"""
        try:
            stock_code = data.get('stock_code')

            if not stock_code:
                await self.send_error(websocket, "缺少股票代码")
                return

            await self.unsubscribe_stock(websocket, stock_code)

            response = {
                'type': 'unsubscribe_success',
                'stock_code': stock_code,
                'timestamp': datetime.now().isoformat()
            }
            await websocket.send(json.dumps(response, ensure_ascii=False))

        except Exception as e:
            logger.error(f"处理取消订阅请求失败: {e}")
            await self.send_error(websocket, f"取消订阅失败: {str(e)}")

    async def handle_ping(self, websocket, data):
        """处理心跳请求"""
        if websocket in self.clients:
            self.clients[websocket]['last_ping'] = time.time()

        pong = {
            'type': 'pong',
            'timestamp': datetime.now().isoformat()
        }
        await websocket.send(json.dumps(pong, ensure_ascii=False))

    async def handle_get_subscriptions(self, websocket):
        """获取当前订阅列表"""
        if websocket in self.clients:
            subscriptions = list(self.clients[websocket]['subscriptions'])
            response = {
                'type': 'subscriptions_list',
                'subscriptions': subscriptions,
                'timestamp': datetime.now().isoformat()
            }
            await websocket.send(json.dumps(response, ensure_ascii=False))

    async def subscribe_stock(self, websocket, stock_code, data_type='price'):
        """订阅股票数据"""
        subscription_key = f"{stock_code}:{data_type}"

        if subscription_key not in self.subscriptions:
            self.subscriptions[subscription_key] = set()

        self.subscriptions[subscription_key].add(websocket)

        if websocket in self.clients:
            self.clients[websocket]['subscriptions'].add(subscription_key)

        logger.info(f"客户端 {self.clients[websocket]['id']} 订阅: {subscription_key}")

    async def unsubscribe_stock(self, websocket, stock_code, data_type=None):
        """取消订阅股票数据"""
        if data_type:
            subscription_key = f"{stock_code}:{data_type}"
            subscription_keys = [subscription_key]
        else:
            # 取消该股票的所有数据类型订阅
            subscription_keys = [key for key in self.subscriptions.keys() if key.startswith(f"{stock_code}:")]

        for key in subscription_keys:
            if key in self.subscriptions and websocket in self.subscriptions[key]:
                self.subscriptions[key].remove(websocket)

                if not self.subscriptions[key]:  # 如果没有客户端订阅了，删除这个key
                    del self.subscriptions[key]

                if websocket in self.clients:
                    self.clients[websocket]['subscriptions'].discard(key)

                logger.info(f"客户端 {self.clients[websocket]['id']} 取消订阅: {key}")

    async def send_error(self, websocket, error_message):
        """发送错误消息"""
        error_msg = {
            'type': 'error',
            'message': error_message,
            'timestamp': datetime.now().isoformat()
        }
        try:
            await websocket.send(json.dumps(error_msg, ensure_ascii=False))
        except:
            pass  # 如果连接已断开，忽略错误

    async def broadcast_to_subscribers(self, subscription_key, data):
        """向订阅者广播数据"""
        if subscription_key not in self.subscriptions:
            return

        message = json.dumps(data, ensure_ascii=False)
        dead_clients = set()

        for websocket in self.subscriptions[subscription_key].copy():
            try:
                await websocket.send(message)
            except websockets.exceptions.ConnectionClosed:
                dead_clients.add(websocket)
            except Exception as e:
                logger.error(f"发送数据到客户端失败: {e}")
                dead_clients.add(websocket)

        # 清理断开的连接
        for websocket in dead_clients:
            await self.unregister_client(websocket)

    def start_data_fetching(self):
        """启动数据获取线程"""
        if not self.is_running:
            self.is_running = True
            self.data_thread = threading.Thread(target=self._data_fetching_loop, daemon=True)
            self.data_thread.start()
            logger.info("数据获取线程已启动")

    def stop_data_fetching(self):
        """停止数据获取线程"""
        self.is_running = False
        if self.data_thread:
            self.data_thread.join()
        logger.info("数据获取线程已停止")

    def _data_fetching_loop(self):
        """数据获取循环"""
        while self.is_running:
            try:
                # 获取所有需要更新的股票代码
                stock_codes = set()
                for key in self.subscriptions.keys():
                    stock_code = key.split(':')[0]
                    stock_codes.add(stock_code)

                if stock_codes:
                    asyncio.run(self._fetch_and_push_data(stock_codes))

                time.sleep(self.update_interval)

            except Exception as e:
                logger.error(f"数据获取循环出错: {e}")
                time.sleep(5)  # 出错时等待5秒再重试

    async def _fetch_and_push_data(self, stock_codes):
        """获取并推送数据"""
        for stock_code in stock_codes:
            try:
                # 获取实时价格数据
                await self._fetch_and_push_price_data(stock_code)

                # 获取实时分笔数据
                await self._fetch_and_push_tick_data(stock_code)

            except Exception as e:
                logger.error(f"获取股票 {stock_code} 数据失败: {e}")

    async def _fetch_and_push_price_data(self, stock_code):
        """获取并推送价格数据"""
        try:
            # 获取实时行情
            realtime_data = ak.stock_zh_a_spot_em()
            stock_data = realtime_data[realtime_data['代码'] == stock_code]

            if not stock_data.empty:
                row = stock_data.iloc[0]

                price_data = {
                    'type': 'price_update',
                    'stock_code': stock_code,
                    'stock_name': row['名称'],
                    'current_price': float(row['最新价']),
                    'change_amount': float(row['涨跌额']),
                    'change_percent': float(row['涨跌幅']),
                    'volume': int(row['成交量']),
                    'amount': float(row['成交额']),
                    'open_price': float(row['今开']),
                    'high_price': float(row['最高']),
                    'low_price': float(row['最低']),
                    'prev_close': float(row['昨收']),
                    'timestamp': datetime.now().isoformat()
                }

                # 检查价格是否有变化
                cache_key = f"{stock_code}:price"
                if cache_key not in self.price_cache or self.price_cache[cache_key]['current_price'] != price_data[
                    'current_price']:
                    self.price_cache[cache_key] = price_data

                    subscription_key = f"{stock_code}:price"
                    await self.broadcast_to_subscribers(subscription_key, price_data)

        except Exception as e:
            logger.error(f"获取股票 {stock_code} 价格数据失败: {e}")

    async def _fetch_and_push_tick_data(self, stock_code):
        """获取并推送分笔数据"""
        try:
            # 获取今日分笔数据的最新几条
            today = datetime.now().strftime('%Y%m%d')
            tick_df = tick_data.get_tick_data(stock_code, today)

            if not tick_df.empty:
                # 获取最新的几条记录
                latest_ticks = tick_df.tail(5).to_dict('records')

                for tick_record in latest_ticks:
                    # 转换时间格式
                    if 'trade_time' in tick_record:
                        tick_record['trade_time'] = str(tick_record['trade_time'])
                    if 'trade_date' in tick_record:
                        tick_record['trade_date'] = str(tick_record['trade_date'])

                tick_data_msg = {
                    'type': 'tick_update',
                    'stock_code': stock_code,
                    'data': latest_ticks,
                    'timestamp': datetime.now().isoformat()
                }

                subscription_key = f"{stock_code}:tick"
                await self.broadcast_to_subscribers(subscription_key, tick_data_msg)

        except Exception as e:
            logger.error(f"获取股票 {stock_code} 分笔数据失败: {e}")

    async def cleanup_dead_connections(self):
        """清理无效连接"""
        current_time = time.time()
        dead_clients = []

        for websocket, client_info in self.clients.items():
            # 如果超过60秒没有ping，认为连接已死
            if current_time - client_info['last_ping'] > 60:
                dead_clients.append(websocket)

        for websocket in dead_clients:
            await self.unregister_client(websocket)

    def get_statistics(self):
        """获取推送服务统计信息"""
        return {
            'total_clients': len(self.clients),
            'total_subscriptions': len(self.subscriptions),
            'subscribed_stocks': len(set(key.split(':')[0] for key in self.subscriptions.keys())),
            'is_running': self.is_running,
            'update_interval': self.update_interval
        }


class RealtimeServer:
    """实时推送服务器"""

    def __init__(self, host='localhost', port=8765):
        self.host = host
        self.port = port
        self.pusher = RealtimePusher()
        self.server = None

    async def start_server(self):
        """启动WebSocket服务器"""
        try:
            self.server = await websockets.serve(
                self.pusher.register_client,
                self.host,
                self.port
            )

            # 启动数据获取线程
            self.pusher.start_data_fetching()

            # 启动连接清理任务
            asyncio.create_task(self._cleanup_task())

            logger.info(f"实时推送服务器已启动: ws://{self.host}:{self.port}")

            # 保持服务器运行
            await self.server.wait_closed()

        except Exception as e:
            logger.error(f"启动实时推送服务器失败: {e}")
            raise

    async def stop_server(self):
        """停止WebSocket服务器"""
        if self.server:
            self.server.close()
            await self.server.wait_closed()

        self.pusher.stop_data_fetching()
        logger.info("实时推送服务器已停止")

    async def _cleanup_task(self):
        """定期清理任务"""
        while True:
            try:
                await self.pusher.cleanup_dead_connections()
                await asyncio.sleep(30)  # 每30秒检查一次
            except Exception as e:
                logger.error(f"清理任务出错: {e}")
                await asyncio.sleep(30)


# 创建全局实例
realtime_pusher = RealtimePusher()
realtime_server = RealtimeServer()