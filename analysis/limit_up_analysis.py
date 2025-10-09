"""
涨停板分析模块
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from loguru import logger
from data.data_fetcher import data_fetcher


class LimitUpAnalyzer:
    """涨停板分析器"""

    def __init__(self):
        self.data_fetcher = data_fetcher

    def generate_limit_up_report(self, trade_date):
        """生成涨停板报告 - 使用实时行情数据"""
        try:
            import time
            # 获取股票列表
            stock_list = self.data_fetcher.get_stock_list()

            if stock_list.empty:
                return {
                    'error': '无法获取股票列表',
                    'limit_up_stocks': [],
                    'total_count': 0
                }

            limit_up_stocks = []

            # API调用计数器
            api_call_count = 0

            # 获取所有股票代码列表
            stock_codes = stock_list['code'].tolist() if 'code' in stock_list.columns else []

            # 分批获取实时数据，避免超时
            batch_size = 50
            for i in range(0, len(stock_codes), batch_size):
                batch_codes = stock_codes[i:i + batch_size]

                try:
                    # 使用实时行情数据
                    realtime_data = self.data_fetcher.get_realtime_data(batch_codes)
                    api_call_count += 1

                    # 每调用10次API后休息1秒
                    if api_call_count % 10 == 0:
                        logger.info(f"已调用API {api_call_count} 次，休息1秒...")
                        time.sleep(1)
                    else:
                        time.sleep(0.2)  # 正常调用间隔0.2秒

                    if not realtime_data.empty:
                        for _, stock in realtime_data.iterrows():
                            try:
                                stock_code = stock.get('code', '')
                                stock_name = stock.get('name', '')
                                current_price = stock.get('current_price', 0)
                                change_pct = stock.get('change_pct', 0)
                                volume = stock.get('volume', 0)
                                amount = stock.get('amount', 0)

                                if not stock_code:
                                    continue

                                # 判断是否涨停（A股一般涨幅限制为10%，ST股为5%）
                                is_st = 'ST' in str(stock_name) or 'st' in str(stock_name).lower()
                                limit_threshold = 4.9 if is_st else 9.9  # 考虑到实际交易中的微小差异

                                if change_pct >= limit_threshold:
                                    limit_up_stocks.append({
                                        'stock_code': stock_code,
                                        'stock_name': stock_name,
                                        'current_price': round(current_price, 2) if current_price else 0,
                                        'change_pct': round(change_pct, 2),
                                        'volume': volume,
                                        'amount': amount,
                                        'limit_up_type': 'ST涨停' if is_st else '普通涨停'
                                    })

                            except Exception as e:
                                logger.warning(f"处理股票 {stock_code} 实时数据失败: {e}")
                                continue

                except Exception as e:
                    logger.error(f"获取第 {i//batch_size + 1} 批实时数据失败: {e}")
                    continue

                logger.info(f"已处理 {min(i + batch_size, len(stock_codes))}/{len(stock_codes)} 只股票")

            # 按涨幅排序
            limit_up_stocks.sort(key=lambda x: x['change_pct'], reverse=True)

            # 分析统计
            analysis = self._analyze_limit_up_pattern(limit_up_stocks)

            result = {
                'trade_date': trade_date,
                'total_count': len(limit_up_stocks),
                'limit_up_stocks': limit_up_stocks,
                'analysis': analysis
            }

            logger.info(f"{trade_date} 涨停板分析完成，共发现 {len(limit_up_stocks)} 只涨停股")
            return result

        except Exception as e:
            logger.error(f"涨停板分析失败: {e}")
            return {
                'error': str(e),
                'limit_up_stocks': [],
                'total_count': 0
            }

    def _analyze_limit_up_pattern(self, limit_up_stocks):
        """分析涨停板模式"""
        if not limit_up_stocks:
            return {
                'st_count': 0,
                'normal_count': 0,
                'avg_volume': 0,
                'high_volume_count': 0
            }

        st_count = sum(1 for stock in limit_up_stocks if stock['limit_up_type'] == 'ST涨停')
        normal_count = len(limit_up_stocks) - st_count

        volumes = [stock['volume'] for stock in limit_up_stocks if stock['volume'] > 0]
        avg_volume = sum(volumes) / len(volumes) if volumes else 0

        # 成交量较大的涨停（简化判断）
        high_volume_count = sum(1 for vol in volumes if vol > avg_volume * 1.5)

        return {
            'st_count': st_count,
            'normal_count': normal_count,
            'avg_volume': int(avg_volume),
            'high_volume_count': high_volume_count,
            'market_sentiment': self._judge_market_sentiment(len(limit_up_stocks))
        }

    def _judge_market_sentiment(self, limit_up_count):
        """判断市场情绪"""
        if limit_up_count >= 100:
            return '极度活跃'
        elif limit_up_count >= 50:
            return '活跃'
        elif limit_up_count >= 20:
            return '一般'
        else:
            return '低迷'


# 创建全局实例
limit_up_analyzer = LimitUpAnalyzer()