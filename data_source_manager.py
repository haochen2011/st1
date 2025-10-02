"""
数据源管理器
支持多个数据源的轮询和自动切换机制
解决连接超时和数据获取稳定性问题
"""

import akshare as ak
import pandas as pd
import time
import random
from functools import wraps
from loguru import logger
from typing import List, Dict, Any, Optional, Callable
import threading
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError


class DataSourceManager:
    """数据源管理器"""

    def __init__(self, timeout=10):
        self.timeout = timeout
        self.current_source = 0
        self.source_status = {}
        self.lock = threading.Lock()

        # 数据源配置
        self.sources = {
            'akshare': {
                'name': 'AKShare',
                'available': True,
                'last_success': time.time(),
                'failure_count': 0,
                'max_failures': 3
            }
        }

        # 尝试导入 baostock
        try:
            import baostock as bs
            self.sources['baostock'] = {
                'name': 'BaoStock',
                'available': True,
                'last_success': time.time(),
                'failure_count': 0,
                'max_failures': 3,
                'session': None
            }
            self._init_baostock()
        except ImportError:
            logger.warning("baostock 包未安装，仅使用 akshare 数据源")

    def _init_baostock(self):
        """初始化 baostock 连接"""
        try:
            import baostock as bs
            lg = bs.login()
            if lg.error_code == '0':
                self.sources['baostock']['session'] = bs
                logger.info("BaoStock 数据源初始化成功")
            else:
                logger.error(f"BaoStock 登录失败: {lg.error_msg}")
                self.sources['baostock']['available'] = False
        except Exception as e:
            logger.error(f"BaoStock 初始化失败: {e}")
            if 'baostock' in self.sources:
                self.sources['baostock']['available'] = False

    def _record_success(self, source_name):
        """记录成功"""
        with self.lock:
            if source_name in self.sources:
                self.sources[source_name]['last_success'] = time.time()
                self.sources[source_name]['failure_count'] = 0
                self.sources[source_name]['available'] = True

    def _record_failure(self, source_name):
        """记录失败"""
        with self.lock:
            if source_name in self.sources:
                self.sources[source_name]['failure_count'] += 1
                if self.sources[source_name]['failure_count'] >= self.sources[source_name]['max_failures']:
                    self.sources[source_name]['available'] = False
                    logger.warning(f"数据源 {source_name} 连续失败 {self.sources[source_name]['failure_count']} 次，暂时禁用")

    def _get_next_available_source(self):
        """获取下一个可用的数据源"""
        available_sources = [name for name, config in self.sources.items() if config['available']]
        if not available_sources:
            # 如果所有数据源都不可用，重置失败计数
            for name in self.sources:
                self.sources[name]['failure_count'] = 0
                self.sources[name]['available'] = True
            available_sources = list(self.sources.keys())

        # 随机选择一个可用的数据源
        return random.choice(available_sources)

    def with_retry_and_fallback(self, func: Callable, *args, **kwargs):
        """装饰器：添加重试和数据源切换机制"""
        max_retries = len(self.sources) * 2  # 每个数据源最多尝试2次

        for attempt in range(max_retries):
            source_name = self._get_next_available_source()

            try:
                logger.debug(f"尝试使用数据源 {source_name} (第 {attempt + 1} 次)")

                # 使用线程池执行，设置超时
                with ThreadPoolExecutor(max_workers=1) as executor:
                    future = executor.submit(func, source_name, *args, **kwargs)
                    result = future.result(timeout=self.timeout)

                self._record_success(source_name)
                return result

            except (FuturesTimeoutError, TimeoutError):
                logger.warning(f"数据源 {source_name} 超时 ({self.timeout}秒)")
                self._record_failure(source_name)

            except Exception as e:
                logger.warning(f"数据源 {source_name} 失败: {e}")
                self._record_failure(source_name)

                # 如果是最后一次尝试，抛出异常
                if attempt == max_retries - 1:
                    raise e

            # 短暂延迟后重试
            time.sleep(random.uniform(0.5, 1.5))

        raise Exception("所有数据源都不可用")

    def get_stock_list(self, market='all'):
        """获取股票列表"""
        def _fetch_stock_list(source_name, market):
            if source_name == 'akshare':
                return self._get_stock_list_akshare(market)
            elif source_name == 'baostock':
                return self._get_stock_list_baostock(market)
            else:
                raise ValueError(f"不支持的数据源: {source_name}")

        return self.with_retry_and_fallback(_fetch_stock_list, market)

    def _get_stock_list_akshare(self, market):
        """使用 akshare 获取股票列表"""
        stocks_list = []

        if market in ['all', 'sh']:
            sh_stocks = ak.stock_info_sh_name_code()
            sh_stocks['market'] = 'sh'
            stocks_list.append(sh_stocks)

        if market in ['all', 'sz']:
            sz_stocks = ak.stock_zh_a_spot_em()
            sz_stocks = sz_stocks[sz_stocks['代码'].str.startswith(('00', '30'))]
            sz_stocks = sz_stocks[['代码', '名称']].rename(
                columns={'代码': 'SECURITY_CODE_A', '名称': 'SECURITY_ABBR_A'})
            sz_stocks['market'] = 'sz'
            stocks_list.append(sz_stocks)

        if stocks_list:
            return pd.concat(stocks_list, ignore_index=True)
        return pd.DataFrame()

    def _get_stock_list_baostock(self, market):
        """使用 baostock 获取股票列表"""
        if 'baostock' not in self.sources or not self.sources['baostock']['available']:
            raise Exception("BaoStock 不可用")

        import baostock as bs

        stocks_list = []

        if market in ['all', 'sh']:
            rs = bs.query_stock_basic(code_name='sh')
            sh_data = []
            while (rs.error_code == '0') & rs.next():
                sh_data.append(rs.get_row_data())

            if sh_data:
                sh_df = pd.DataFrame(sh_data, columns=rs.fields)
                sh_df = sh_df.rename(columns={'code': 'SECURITY_CODE_A', 'code_name': 'SECURITY_ABBR_A'})
                sh_df['market'] = 'sh'
                stocks_list.append(sh_df)

        if market in ['all', 'sz']:
            rs = bs.query_stock_basic(code_name='sz')
            sz_data = []
            while (rs.error_code == '0') & rs.next():
                sz_data.append(rs.get_row_data())

            if sz_data:
                sz_df = pd.DataFrame(sz_data, columns=rs.fields)
                sz_df = sz_df.rename(columns={'code': 'SECURITY_CODE_A', 'code_name': 'SECURITY_ABBR_A'})
                sz_df['market'] = 'sz'
                stocks_list.append(sz_df)

        if stocks_list:
            return pd.concat(stocks_list, ignore_index=True)
        return pd.DataFrame()

    def get_stock_basic_info(self, stock_code):
        """获取股票基本信息"""
        def _fetch_basic_info(source_name, stock_code):
            if source_name == 'akshare':
                return self._get_basic_info_akshare(stock_code)
            elif source_name == 'baostock':
                return self._get_basic_info_baostock(stock_code)
            else:
                raise ValueError(f"不支持的数据源: {source_name}")

        return self.with_retry_and_fallback(_fetch_basic_info, stock_code)

    def _get_basic_info_akshare(self, stock_code):
        """使用 akshare 获取股票基本信息"""
        try:
            # 获取股票基本信息
            stock_individual_info = ak.stock_individual_info_em(symbol=stock_code)

            # 解析基本信息
            info_dict = {}
            for _, row in stock_individual_info.iterrows():
                info_dict[row['item']] = row['value']

            # 获取行业信息 - 多种方式尝试
            industry = self._get_industry_info_akshare(stock_code, info_dict)

            basic_info = {
                'stock_code': stock_code,
                'stock_name': info_dict.get('股票简称', ''),
                'market': 'sh' if stock_code.startswith('6') else 'sz',
                'list_date': self._parse_date(info_dict.get('上市时间')),
                'total_shares': self._parse_number(info_dict.get('总股本')),
                'float_shares': self._parse_number(info_dict.get('流通股')),
                'industry': industry
            }

            return basic_info

        except Exception as e:
            logger.error(f"AKShare 获取股票 {stock_code} 基本信息失败: {e}")
            raise

    def _get_industry_info_akshare(self, stock_code, info_dict):
        """使用多种方式获取行业信息"""
        industry = info_dict.get('所属行业', '')

        # 如果行业信息为空，尝试其他方式获取
        if not industry or industry in ['-', '']:
            try:
                # 尝试从概念板块获取
                concept_data = ak.stock_board_concept_cons_em(symbol="华为概念")  # 示例，实际需要动态获取
                # 这里可以添加更多获取行业信息的逻辑
                pass
            except:
                pass

            # 如果仍为空，尝试从其他接口获取
            if not industry:
                try:
                    # 尝试从股票实时数据获取
                    real_data = ak.stock_zh_a_spot_em()
                    stock_data = real_data[real_data['代码'] == stock_code]
                    if not stock_data.empty and '所属行业' in stock_data.columns:
                        industry = stock_data.iloc[0]['所属行业']
                except:
                    pass

        # 如果还是为空，设置为未知
        if not industry or industry in ['-', '', 'nan', 'None']:
            industry = '未分类'

        return industry

    def _get_basic_info_baostock(self, stock_code):
        """使用 baostock 获取股票基本信息"""
        if 'baostock' not in self.sources or not self.sources['baostock']['available']:
            raise Exception("BaoStock 不可用")

        import baostock as bs

        # 格式化股票代码
        if stock_code.startswith('6'):
            bs_code = f"sh.{stock_code}"
        else:
            bs_code = f"sz.{stock_code}"

        # 获取基本信息
        rs = bs.query_stock_basic(code=bs_code)

        if rs.error_code != '0':
            raise Exception(f"BaoStock 查询失败: {rs.error_msg}")

        data = []
        while rs.next():
            data.append(rs.get_row_data())

        if not data:
            raise Exception(f"未找到股票 {stock_code} 的基本信息")

        info = dict(zip(rs.fields, data[0]))

        basic_info = {
            'stock_code': stock_code,
            'stock_name': info.get('code_name', ''),
            'market': 'sh' if stock_code.startswith('6') else 'sz',
            'list_date': self._parse_date(info.get('ipoDate')),
            'total_shares': self._parse_number(info.get('outDate')),  # baostock 字段可能不同
            'float_shares': None,  # baostock 可能没有这个字段
            'industry': info.get('industry', '未分类')
        }

        return basic_info

    def get_stock_data(self, stock_code, period='daily', start_date=None, end_date=None, adjust='qfq'):
        """获取股票数据"""
        def _fetch_stock_data(source_name, stock_code, period, start_date, end_date, adjust):
            if source_name == 'akshare':
                return self._get_stock_data_akshare(stock_code, period, start_date, end_date, adjust)
            elif source_name == 'baostock':
                return self._get_stock_data_baostock(stock_code, period, start_date, end_date, adjust)
            else:
                raise ValueError(f"不支持的数据源: {source_name}")

        return self.with_retry_and_fallback(_fetch_stock_data, stock_code, period, start_date, end_date, adjust)

    def _get_stock_data_akshare(self, stock_code, period, start_date, end_date, adjust):
        """使用 akshare 获取股票数据"""
        if period in ['1min', '5min', '15min', '30min', '60min']:
            return ak.stock_zh_a_hist_min_em(
                symbol=stock_code,
                start_date=start_date,
                end_date=end_date,
                period=period.replace('min', ''),
                adjust=adjust
            )
        else:
            period_mapping = {'daily': 'daily', 'week': 'weekly', 'month': 'monthly'}
            ak_period = period_mapping.get(period, 'daily')
            return ak.stock_zh_a_hist(
                symbol=stock_code,
                period=ak_period,
                start_date=start_date,
                end_date=end_date,
                adjust=adjust
            )

    def _get_stock_data_baostock(self, stock_code, period, start_date, end_date, adjust):
        """使用 baostock 获取股票数据"""
        if 'baostock' not in self.sources or not self.sources['baostock']['available']:
            raise Exception("BaoStock 不可用")

        import baostock as bs

        # 格式化股票代码
        if stock_code.startswith('6'):
            bs_code = f"sh.{stock_code}"
        else:
            bs_code = f"sz.{stock_code}"

        # 转换周期
        period_mapping = {
            'daily': 'd', 'week': 'w', 'month': 'm',
            '5min': '5', '15min': '15', '30min': '30', '60min': '60'
        }
        bs_period = period_mapping.get(period, 'd')

        # 查询数据
        if period in ['5min', '15min', '30min', '60min']:
            rs = bs.query_history_k_data_plus(
                bs_code,
                "date,time,code,open,high,low,close,volume,amount",
                start_date=start_date, end_date=end_date,
                frequency=bs_period, adjustflag="3"  # 不复权
            )
        else:
            rs = bs.query_history_k_data_plus(
                bs_code,
                "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST",
                start_date=start_date, end_date=end_date,
                frequency=bs_period, adjustflag="3"
            )

        if rs.error_code != '0':
            raise Exception(f"BaoStock 查询失败: {rs.error_msg}")

        data = []
        while rs.next():
            data.append(rs.get_row_data())

        if data:
            df = pd.DataFrame(data, columns=rs.fields)
            # 转换数据类型和列名以匹配 akshare 格式
            df = self._convert_baostock_format(df)
            return df

        return pd.DataFrame()

    def _convert_baostock_format(self, df):
        """转换 baostock 数据格式为统一格式"""
        column_mapping = {
            'date': '日期',
            'open': '开盘',
            'high': '最高',
            'low': '最低',
            'close': '收盘',
            'volume': '成交量',
            'amount': '成交额',
            'pctChg': '涨跌幅',
            'turn': '换手率'
        }

        for old_col, new_col in column_mapping.items():
            if old_col in df.columns:
                df = df.rename(columns={old_col: new_col})

        # 转换数据类型
        numeric_columns = ['开盘', '最高', '最低', '收盘', '成交量', '成交额', '涨跌幅', '换手率']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        return df

    def _parse_date(self, date_str):
        """解析日期字符串"""
        if not date_str or date_str == '-':
            return None
        try:
            from datetime import datetime
            return datetime.strptime(str(date_str), '%Y-%m-%d').date()
        except:
            try:
                return datetime.strptime(str(date_str), '%Y%m%d').date()
            except:
                return None

    def _parse_number(self, num_str):
        """解析数字字符串"""
        if not num_str or num_str == '-':
            return None
        try:
            num_str = str(num_str).replace(',', '').replace(' ', '')
            if '万' in num_str:
                return float(num_str.replace('万', '')) * 10000
            elif '亿' in num_str:
                return float(num_str.replace('亿', '')) * 100000000
            else:
                return float(num_str)
        except:
            return None

    def get_source_status(self):
        """获取数据源状态"""
        return self.sources.copy()

    def reset_source_status(self):
        """重置数据源状态"""
        with self.lock:
            for name in self.sources:
                self.sources[name]['failure_count'] = 0
                self.sources[name]['available'] = True
        logger.info("已重置所有数据源状态")

    def close(self):
        """关闭数据源连接"""
        if 'baostock' in self.sources and self.sources['baostock'].get('session'):
            try:
                import baostock as bs
                bs.logout()
                logger.info("BaoStock 连接已关闭")
            except:
                pass


# 创建全局数据源管理器实例
data_source_manager = DataSourceManager()
