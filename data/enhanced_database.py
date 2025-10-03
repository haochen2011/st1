#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
增强的数据库管理器
优化批量插入和查询性能，支持事务管理和连接池
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text, MetaData, Table, Column, Integer, String, Float, Date, DateTime, Text
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.engine import Engine
from sqlalchemy.pool import QueuePool
import pymysql
from typing import Dict, List, Optional, Union, Any
from datetime import datetime, date
import json
from loguru import logger
from core.config import config

Base = declarative_base()


class EnhancedDatabaseManager:
    """增强的数据库管理器"""

    def __init__(self):
        self.engine: Optional[Engine] = None
        self.Session = None
        self.metadata = MetaData()
        self._connection_pool = None
        self.init_database()

    def init_database(self):
        """初始化数据库连接"""
        try:
            # 构建数据库连接字符串
            db_config = {
                'host': config.get('database', 'host', 'localhost'),
                'port': config.getint('database', 'port', 3306),
                'user': config.get('database', 'user', 'root'),
                'password': config.get('database', 'password', ''),
                'database': config.get('database', 'database', 'stock_dragon'),
                'charset': 'utf8mb4'
            }

            connection_string = (
                f"mysql+pymysql://{db_config['user']}:{db_config['password']}"
                f"@{db_config['host']}:{db_config['port']}/{db_config['database']}"
                f"?charset={db_config['charset']}"
            )

            # 创建引擎，使用连接池
            self.engine = create_engine(
                connection_string,
                poolclass=QueuePool,
                pool_size=20,
                max_overflow=30,
                pool_pre_ping=True,
                pool_recycle=3600,
                echo=False
            )

            # 创建会话工厂
            self.Session = sessionmaker(bind=self.engine)

            # 测试连接
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))

            logger.info("数据库连接初始化成功")

            # 创建表结构
            self.create_tables()

        except Exception as e:
            logger.error(f"数据库连接初始化失败: {e}")
            raise

    def create_tables(self):
        """创建表结构"""
        try:
            # 股票基本信息表
            stock_info_ddl = """
            CREATE TABLE IF NOT EXISTS stock_info (
                id INT AUTO_INCREMENT PRIMARY KEY,
                stock_code VARCHAR(10) NOT NULL UNIQUE,
                stock_name VARCHAR(50) NOT NULL,
                market VARCHAR(10) NOT NULL,
                list_date DATE,
                total_shares BIGINT,
                float_shares BIGINT,
                industry VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                INDEX idx_stock_code (stock_code),
                INDEX idx_market (market),
                INDEX idx_industry (industry)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """



            # 技术指标表
            indicator_data_ddl = """
            CREATE TABLE IF NOT EXISTS indicator_data (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                stock_code VARCHAR(10) NOT NULL,
                period VARCHAR(20) NOT NULL,
                trade_date DATE NOT NULL,
                indicator_name VARCHAR(50) NOT NULL,
                indicator_value DECIMAL(15,6),
                indicator_data JSON,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY uk_stock_period_date_indicator (stock_code, period, trade_date, indicator_name),
                INDEX idx_stock_code (stock_code),
                INDEX idx_indicator_name (indicator_name),
                INDEX idx_trade_date (trade_date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """

            # 执行DDL语句
            with self.engine.connect() as conn:
                with conn.begin():
                    conn.execute(text(stock_info_ddl))
                    conn.execute(text(indicator_data_ddl))

            logger.info("数据库表结构创建/验证完成")

        except Exception as e:
            logger.error(f"创建数据库表失败: {e}")
            raise

    def create_tick_data_table(self, trade_date):
        """创建按日期分表的分笔数据表"""
        try:
            # 格式化日期为字符串，例如：tick_data_20251002
            if isinstance(trade_date, str):
                date_str = trade_date.replace('-', '')
            else:
                date_str = trade_date.strftime('%Y%m%d')

            table_name = f"tick_data_{date_str}"

            tick_data_ddl = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                stock_code VARCHAR(10) NOT NULL,
                trade_date DATE NOT NULL,
                trade_time DATETIME NOT NULL,
                price DECIMAL(10,3),
                price_change DECIMAL(10,3),
                volume INT,
                amount DECIMAL(15,2),
                trade_type VARCHAR(20),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_stock_date (stock_code, trade_date),
                INDEX idx_trade_time (trade_time),
                INDEX idx_stock_code (stock_code)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """

            with self.engine.connect() as conn:
                with conn.begin():
                    conn.execute(text(tick_data_ddl))

            logger.info(f"分笔数据表 {table_name} 创建成功")
            return table_name

        except Exception as e:
            logger.error(f"创建分笔数据表失败: {e}")
            return None

    def create_basic_data_table(self, period):
        """创建按周期分表的基础数据表"""
        try:
            # 支持的周期：1min,5min,10min,15min,30min,1hour,daily,week,month,quarter,half-year,year
            valid_periods = ['1min', '5min', '10min', '15min', '30min', '1hour',
                           'daily', 'week', 'month', 'quarter', 'half-year', 'year']

            if period not in valid_periods:
                logger.error(f"不支持的周期: {period}")
                return None

            # 将period中的特殊字符替换为下划线
            safe_period = period.replace('-', '_')
            table_name = f"basic_data_{safe_period}"

            basic_data_ddl = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                stock_code VARCHAR(10) NOT NULL,
                trade_date DATE NOT NULL,
                trade_time DATETIME,
                open_price DECIMAL(10,3),
                high_price DECIMAL(10,3),
                low_price DECIMAL(10,3),
                close_price DECIMAL(10,3),
                volume BIGINT,
                amount DECIMAL(20,2),
                turnover_rate DECIMAL(8,4),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY uk_stock_date (stock_code, trade_date),
                INDEX idx_stock_code (stock_code),
                INDEX idx_trade_date (trade_date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """

            with self.engine.connect() as conn:
                with conn.begin():
                    conn.execute(text(basic_data_ddl))

            logger.info(f"基础数据表 {table_name} 创建成功")
            return table_name

        except Exception as e:
            logger.error(f"创建基础数据表失败: {e}")
            return None

    def get_tick_table_name(self, trade_date):
        """获取分笔数据表名"""
        if isinstance(trade_date, str):
            date_str = trade_date.replace('-', '')
        else:
            date_str = trade_date.strftime('%Y%m%d')
        return f"tick_data_{date_str}"

    def get_basic_table_name(self, period):
        """获取基础数据表名"""
        safe_period = period.replace('-', '_')
        return f"basic_data_{safe_period}"

    def batch_insert_dataframe(self,
                               df: pd.DataFrame,
                               table_name: str,
                               if_exists: str = 'append',
                               batch_size: int = 1000,
                               method: str = 'multi') -> bool:
        """
        批量插入DataFrame数据，优化性能

        Args:
            df: 要插入的DataFrame
            table_name: 目标表名
            if_exists: 如果表存在时的操作 ('fail', 'replace', 'append')
            batch_size: 批次大小
            method: 插入方法 ('multi' 或 None)
        """
        try:
            if df.empty:
                logger.warning(f"DataFrame为空，跳过插入到表 {table_name}")
                return True

            # 数据预处理
            df_clean = self._preprocess_dataframe(df)

            # 分批插入
            total_rows = len(df_clean)
            inserted_rows = 0

            for i in range(0, total_rows, batch_size):
                batch_df = df_clean.iloc[i:i + batch_size].copy()

                try:
                    # 使用to_sql进行批量插入
                    batch_df.to_sql(
                        name=table_name,
                        con=self.engine,
                        if_exists=if_exists if i == 0 else 'append',
                        index=False,
                        method=method,
                        chunksize=min(batch_size, 500)
                    )

                    inserted_rows += len(batch_df)

                    if total_rows > batch_size:
                        logger.info(f"已插入 {inserted_rows}/{total_rows} 行到表 {table_name}")

                except Exception as e:
                    logger.error(f"批次插入失败 (行 {i}-{i + len(batch_df)}): {e}")
                    # 继续处理下一批次
                    continue

            logger.info(f"批量插入完成: {inserted_rows}/{total_rows} 行到表 {table_name}")
            return inserted_rows > 0

        except Exception as e:
            logger.error(f"批量插入DataFrame失败: {e}")
            return False

    def _preprocess_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """预处理DataFrame数据"""
        try:
            df_clean = df.copy()

            # 处理无穷大和NaN值
            df_clean = df_clean.replace([np.inf, -np.inf], np.nan)

            # 对于数值列，填充NaN为None
            numeric_columns = df_clean.select_dtypes(include=[np.number]).columns
            for col in numeric_columns:
                df_clean[col] = df_clean[col].where(pd.notnull(df_clean[col]), None)

            # 处理日期列
            date_columns = ['trade_date', 'list_date']
            for col in date_columns:
                if col in df_clean.columns:
                    df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce')

            # 处理时间列
            datetime_columns = ['trade_time', 'created_at', 'updated_at']
            for col in datetime_columns:
                if col in df_clean.columns:
                    df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce')

            return df_clean

        except Exception as e:
            logger.error(f"预处理DataFrame失败: {e}")
            return df

    def execute_sql(self, sql: str, params: Optional[Dict] = None) -> bool:
        """执行SQL语句"""
        try:
            with self.engine.connect() as conn:
                with conn.begin():
                    if params:
                        conn.execute(text(sql), params)
                    else:
                        conn.execute(text(sql))
            return True

        except Exception as e:
            logger.error(f"执行SQL失败: {e}")
            return False

    def query_to_dataframe(self, sql: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """查询数据并返回DataFrame"""
        try:
            with self.engine.connect() as conn:
                if params:
                    df = pd.read_sql(text(sql), conn, params=params)
                else:
                    df = pd.read_sql(text(sql), conn)

                return df

        except Exception as e:
            logger.error(f"查询数据失败: {e}")
            return pd.DataFrame()

    def table_exists(self, table_name: str) -> bool:
        """检查表是否存在"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(f"SHOW TABLES LIKE '{table_name}'"))
                return result.fetchone() is not None
        except Exception as e:
            logger.error(f"检查表是否存在失败: {e}")
            return False

    def safe_query_to_dataframe(self, sql: str, params: Optional[Dict] = None, required_tables: List[str] = None) -> pd.DataFrame:
        """安全查询数据，检查表是否存在"""
        try:
            # 检查必需的表是否存在
            if required_tables:
                for table in required_tables:
                    if not self.table_exists(table):
                        logger.warning(f"表 {table} 不存在，跳过查询")
                        return pd.DataFrame()

            return self.query_to_dataframe(sql, params)
        except Exception as e:
            logger.error(f"安全查询数据失败: {e}")
            return pd.DataFrame()

    def upsert_dataframe(self,
                         df: pd.DataFrame,
                         table_name: str,
                         unique_columns: List[str],
                         batch_size: int = 1000) -> bool:
        """
        批量更新插入数据（如果存在则更新，不存在则插入）

        Args:
            df: 要插入的DataFrame
            table_name: 目标表名
            unique_columns: 唯一键列名列表
            batch_size: 批次大小
        """
        try:
            if df.empty:
                return True

            # 生成UPSERT SQL
            columns = list(df.columns)
            placeholders = ', '.join([f':{col}' for col in columns])

            # 构建ON DUPLICATE KEY UPDATE子句
            update_clause = ', '.join([
                f'{col} = VALUES({col})' for col in columns
                if col not in unique_columns and col not in ['id', 'created_at']
            ])

            sql = f"""
            INSERT INTO {table_name} ({', '.join(columns)})
            VALUES ({placeholders})
            ON DUPLICATE KEY UPDATE {update_clause}
            """

            # 分批执行
            total_rows = len(df)
            processed_rows = 0

            with self.engine.connect() as conn:
                with conn.begin():
                    for i in range(0, total_rows, batch_size):
                        batch_df = df.iloc[i:i + batch_size]
                        batch_data = batch_df.to_dict('records')

                        conn.execute(text(sql), batch_data)
                        processed_rows += len(batch_df)

                        if total_rows > batch_size:
                            logger.info(f"已处理 {processed_rows}/{total_rows} 行 (UPSERT)")

            logger.info(f"UPSERT完成: {processed_rows} 行到表 {table_name}")
            return True

        except Exception as e:
            logger.error(f"UPSERT失败: {e}")
            return False

    def get_table_info(self, table_name: str) -> Dict:
        """获取表信息"""
        try:
            sql = f"""
            SELECT
                COUNT(*) as total_rows,
                MIN(created_at) as earliest_data,
                MAX(created_at) as latest_data
            FROM {table_name}
            """

            result = self.query_to_dataframe(sql)
            if not result.empty:
                return result.iloc[0].to_dict()
            return {}

        except Exception as e:
            logger.error(f"获取表信息失败: {e}")
            return {}

    def optimize_table(self, table_name: str) -> bool:
        """优化表结构"""
        try:
            with self.engine.connect() as conn:
                with conn.begin():
                    # 分析表
                    conn.execute(text(f"ANALYZE TABLE {table_name}"))
                    # 优化表
                    conn.execute(text(f"OPTIMIZE TABLE {table_name}"))

            logger.info(f"表 {table_name} 优化完成")
            return True

        except Exception as e:
            logger.error(f"优化表失败: {e}")
            return False

    def cleanup_old_data(self, table_name: str, date_column: str, days_to_keep: int = 365) -> bool:
        """清理旧数据"""
        try:
            sql = f"""
            DELETE FROM {table_name}
            WHERE {date_column} < DATE_SUB(NOW(), INTERVAL {days_to_keep} DAY)
            """

            with self.engine.connect() as conn:
                with conn.begin():
                    result = conn.execute(text(sql))
                    deleted_rows = result.rowcount

            logger.info(f"清理完成: 从表 {table_name} 删除了 {deleted_rows} 行旧数据")
            return True

        except Exception as e:
            logger.error(f"清理旧数据失败: {e}")
            return False

    def batch_insert_to_dynamic_table(self,
                                     df: pd.DataFrame,
                                     table_type: str,
                                     date_or_period: str,
                                     if_exists: str = 'append',
                                     batch_size: int = 1000,
                                     method: str = 'multi') -> bool:
        """
        批量插入数据到动态表中

        Args:
            df: 要插入的DataFrame
            table_type: 表类型 ('tick' 或 'basic')
            date_or_period: 日期（分笔数据）或周期（基础数据）
            if_exists: 如果表存在时的操作
            batch_size: 批次大小
            method: 插入方法
        """
        try:
            if table_type == 'tick':
                table_name = self.create_tick_data_table(date_or_period)
            elif table_type == 'basic':
                table_name = self.create_basic_data_table(date_or_period)
            else:
                raise ValueError(f"不支持的表类型: {table_type}")

            if not table_name:
                return False

            return self.batch_insert_dataframe(df, table_name, if_exists, batch_size, method)

        except Exception as e:
            logger.error(f"批量插入数据到动态表失败: {e}")
            return False

    def close(self):
        """关闭数据库连接"""
        try:
            if self.engine:
                self.engine.dispose()
            logger.info("数据库连接已关闭")

        except Exception as e:
            logger.error(f"关闭数据库连接失败: {e}")


# 创建全局增强数据库管理器实例
enhanced_db_manager = EnhancedDatabaseManager()
