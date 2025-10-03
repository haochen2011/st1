"""
股票信息管理模块 (F10数据)
负责股票基本信息的获取、存储和管理
"""

import akshare as ak
import pandas as pd
import numpy as np
from datetime import datetime, date
from loguru import logger
try:
    from enhanced_database import enhanced_db_manager as db_manager
except ImportError:
    from database import db_manager
from config import config


class StockInfo:
    """股票信息管理类"""

    def __init__(self):
        self.market_codes = config.get_market_codes()

    def get_stock_list(self, market='all'):
        """获取股票列表"""
        try:
            if market == 'all' or market == 'sh':
                # 获取上海股票列表
                sh_stocks = ak.stock_info_sh_name_code()
                sh_stocks['market'] = 'sh'

            if market == 'all' or market == 'sz':
                # 获取深圳股票列表
                sz_stocks = ak.stock_zh_a_spot_em()
                sz_stocks = sz_stocks[sz_stocks['代码'].str.startswith(('00', '30'))]
                sz_stocks = sz_stocks[['代码', '名称']].rename(
                    columns={'代码': 'SECURITY_CODE_A', '名称': 'SECURITY_ABBR_A'})
                sz_stocks['market'] = 'sz'

            if market == 'all':
                stocks = pd.concat([sh_stocks, sz_stocks], ignore_index=True)
            elif market == 'sh':
                stocks = sh_stocks
            elif market == 'sz':
                stocks = sz_stocks
            else:
                raise ValueError(f"不支持的市场代码: {market}")

            logger.info(f"获取 {market} 市场股票列表成功，共 {len(stocks)} 只股票")
            return stocks

        except Exception as e:
            logger.error(f"获取股票列表失败: {e}")
            return pd.DataFrame()

    def get_stock_basic_info(self, stock_code):
        """获取股票基本信息"""
        try:
            # 获取股票基本信息
            stock_individual_info = ak.stock_individual_info_em(symbol=stock_code)

            # 解析基本信息
            info_dict = {}
            for _, row in stock_individual_info.iterrows():
                info_dict[row['item']] = row['value']

            # 标准化信息
            basic_info = {
                'stock_code': stock_code,
                'stock_name': info_dict.get('股票简称', ''),
                'market': 'sh' if stock_code.startswith('6') else 'sz',
                'list_date': self._parse_date(info_dict.get('上市时间')),
                'total_shares': self._parse_number(info_dict.get('总股本')),
                'float_shares': self._parse_number(info_dict.get('流通股')),
                'industry': info_dict.get('行业', '')
            }

            logger.info(f"获取股票 {stock_code} 基本信息成功")
            return basic_info

        except Exception as e:
            logger.error(f"获取股票 {stock_code} 基本信息失败: {e}")
            return None

    def get_stock_financial_data(self, stock_code, year=None):
        """获取股票财务数据"""
        try:
            if year is None:
                year = datetime.now().year

            # 获取财务数据
            financial_data = ak.stock_financial_em(symbol=stock_code)

            if not financial_data.empty:
                # 筛选指定年份的数据
                financial_data['报告期'] = pd.to_datetime(financial_data['报告期'])
                year_data = financial_data[financial_data['报告期'].dt.year == year]

                logger.info(f"获取股票 {stock_code} {year}年财务数据成功")
                return year_data
            else:
                logger.warning(f"股票 {stock_code} 无财务数据")
                return pd.DataFrame()

        except Exception as e:
            logger.error(f"获取股票 {stock_code} 财务数据失败: {e}")
            return pd.DataFrame()

    def update_stock_info_to_db(self, stock_list=None):
        """更新股票信息到数据库"""
        if stock_list is None:
            stock_list = self.get_stock_list()

        success_count = 0
        for _, stock in stock_list.iterrows():
            try:
                stock_code = stock['SECURITY_CODE_A']
                basic_info = self.get_stock_basic_info(stock_code)

                if basic_info:
                    # 插入或更新数据库
                    sql = """
                    INSERT INTO stock_info (stock_code, stock_name, market, list_date, total_shares, float_shares, industry)
                    VALUES (:stock_code, :stock_name, :market, :list_date, :total_shares, :float_shares, :industry)
                    ON DUPLICATE KEY UPDATE
                    stock_name = VALUES(stock_name),
                    list_date = VALUES(list_date),
                    total_shares = VALUES(total_shares),
                    float_shares = VALUES(float_shares),
                    industry = VALUES(industry),
                    updated_at = CURRENT_TIMESTAMP
                    """

                    db_manager.execute_sql(sql, basic_info)
                    success_count += 1

            except Exception as e:
                logger.error(f"更新股票 {stock_code} 信息失败: {e}")
                continue

        logger.info(f"成功更新 {success_count} 只股票信息到数据库")
        return success_count

    def get_stock_info_from_db(self, stock_code=None):
        """从数据库获取股票信息"""
        if stock_code:
            sql = "SELECT * FROM stock_info WHERE stock_code = :stock_code"
            params = {'stock_code': stock_code}
        else:
            sql = "SELECT * FROM stock_info ORDER BY stock_code"
            params = None

        return db_manager.query_to_dataframe(sql, params)

    def _parse_date(self, date_str):
        """解析日期字符串"""
        if not date_str or date_str == '-':
            return None
        try:
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
            # 处理带单位的数字（如：1.23万、4.56亿）
            num_str = str(num_str).replace(',', '').replace(' ', '')
            if '万' in num_str:
                return float(num_str.replace('万', '')) * 10000
            elif '亿' in num_str:
                return float(num_str.replace('亿', '')) * 100000000
            else:
                return float(num_str)
        except:
            return None

    def calculate_market_value(self, stock_code, price=None):
        """计算市值"""
        try:
            stock_info = self.get_stock_info_from_db(stock_code)
            if stock_info.empty:
                return None

            total_shares = stock_info.iloc[0]['total_shares']
            float_shares = stock_info.iloc[0]['float_shares']

            if price is None:
                # 获取最新价格
                from basic_data import BasicData
                basic_data = BasicData()
                latest_data = basic_data.get_latest_data(stock_code)
                if latest_data.empty:
                    return None
                price = latest_data.iloc[0]['close_price']

            market_value = {
                'total_market_value': total_shares * price if total_shares else None,
                'float_market_value': float_shares * price if float_shares else None,
                'price': price
            }

            return market_value

        except Exception as e:
            logger.error(f"计算股票 {stock_code} 市值失败: {e}")
            return None

    def save_stock_info_to_db(self, stock_info_dict):
        """保存单个股票信息到数据库（支持增强数据库管理器）"""
        try:
            if hasattr(db_manager, 'upsert_dataframe'):
                # 使用增强数据库管理器的upsert功能
                df = pd.DataFrame([stock_info_dict])
                return db_manager.upsert_dataframe(
                    df, 'stock_info',
                    unique_columns=['stock_code']
                )
            else:
                # 使用传统的SQL方式
                sql = """
                INSERT INTO stock_info (stock_code, stock_name, market, list_date, total_shares, float_shares, industry)
                VALUES (:stock_code, :stock_name, :market, :list_date, :total_shares, :float_shares, :industry)
                ON DUPLICATE KEY UPDATE
                stock_name = VALUES(stock_name),
                list_date = VALUES(list_date),
                total_shares = VALUES(total_shares),
                float_shares = VALUES(float_shares),
                industry = VALUES(industry),
                updated_at = CURRENT_TIMESTAMP
                """
                return db_manager.execute_sql(sql, stock_info_dict)

        except Exception as e:
            logger.error(f"保存股票信息到数据库失败: {e}")
            return False

    def batch_save_stock_info_to_db(self, stock_info_list):
        """批量保存股票信息到数据库"""
        try:
            if not stock_info_list:
                return False

            # 转换为DataFrame
            df = pd.DataFrame(stock_info_list)

            if hasattr(db_manager, 'batch_insert_dataframe'):
                # 使用增强数据库管理器的批量插入
                return db_manager.batch_insert_dataframe(
                    df, 'stock_info',
                    if_exists='append',
                    batch_size=100
                )
            else:
                # 逐个插入
                success_count = 0
                for stock_info_dict in stock_info_list:
                    if self.save_stock_info_to_db(stock_info_dict):
                        success_count += 1

                logger.info(f"批量保存完成: 成功 {success_count}/{len(stock_info_list)}")
                return success_count > 0

        except Exception as e:
            logger.error(f"批量保存股票信息失败: {e}")
            return False

    def get_all_stock_codes_from_db(self):
        """从数据库获取所有股票代码"""
        try:
            sql = "SELECT stock_code, stock_name, market FROM stock_info ORDER BY stock_code"
            return db_manager.query_to_dataframe(sql)
        except Exception as e:
            logger.error(f"从数据库获取股票代码失败: {e}")
            return pd.DataFrame()

    def update_stock_industry_batch(self, stock_industry_dict):
        """批量更新股票行业信息"""
        try:
            update_list = []
            for stock_code, industry in stock_industry_dict.items():
                update_list.append({
                    'stock_code': stock_code,
                    'industry': industry
                })

            if hasattr(db_manager, 'upsert_dataframe'):
                df = pd.DataFrame(update_list)
                return db_manager.upsert_dataframe(
                    df, 'stock_info',
                    unique_columns=['stock_code']
                )
            else:
                # 传统方式更新
                success_count = 0
                for item in update_list:
                    sql = """
                    UPDATE stock_info
                    SET industry = :industry, updated_at = CURRENT_TIMESTAMP
                    WHERE stock_code = :stock_code
                    """
                    if db_manager.execute_sql(sql, item):
                        success_count += 1

                logger.info(f"批量更新行业信息完成: 成功 {success_count}/{len(update_list)}")
                return success_count > 0

        except Exception as e:
            logger.error(f"批量更新股票行业信息失败: {e}")
            return False

    def get_stocks_by_market(self, market='all'):
        """根据市场获取股票列表"""
        try:
            if market == 'all':
                sql = "SELECT * FROM stock_info ORDER BY market, stock_code"
                params = None
            else:
                sql = "SELECT * FROM stock_info WHERE market = :market ORDER BY stock_code"
                params = {'market': market}

            return db_manager.query_to_dataframe(sql, params)

        except Exception as e:
            logger.error(f"获取市场 {market} 股票列表失败: {e}")
            return pd.DataFrame()

    def get_stocks_by_industry(self, industry):
        """根据行业获取股票列表"""
        try:
            sql = "SELECT * FROM stock_info WHERE industry = :industry ORDER BY stock_code"
            params = {'industry': industry}
            return db_manager.query_to_dataframe(sql, params)

        except Exception as e:
            logger.error(f"获取行业 {industry} 股票列表失败: {e}")
            return pd.DataFrame()


# 创建全局实例
stock_info = StockInfo()