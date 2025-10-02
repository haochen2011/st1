#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库管理模块
负责MySQL数据库连接和操作
"""

import pymysql
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
from loguru import logger
from config import config


class DatabaseManager:
    """数据库管理类"""

    def __init__(self):
        self.host = config.get('database', 'host')
        self.port = config.getint('database', 'port')
        self.user = config.get('database', 'user')
        self.password = config.get('database', 'password')
        self.database = config.get('database', 'database')

        self.engine = None
        self.Session = None
        self._init_database()

    def _init_database(self):
        """初始化数据库连接"""
        try:
            # 创建数据库引擎
            connection_string = f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}?charset=utf8mb4"
            self.engine = create_engine(connection_string, echo=False, pool_pre_ping=True)

            # 创建会话
            self.Session = sessionmaker(bind=self.engine)

            # 测试连接
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("数据库连接成功")

            # 创建必要的表
            self._create_tables()

        except Exception as e:
            logger.error(f"数据库连接失败: {e}")

    def _create_tables(self):
        """创建必要的数据库表"""
        create_tables_sql = [
            # 股票基本信息表
            """
            CREATE TABLE IF NOT EXISTS stock_info (
                id INT AUTO_INCREMENT PRIMARY KEY,
                stock_code VARCHAR(20) NOT NULL,
                stock_name VARCHAR(100) NOT NULL,
                market VARCHAR(10) NOT NULL,
                list_date DATE,
                total_shares BIGINT,
                float_shares BIGINT,
                industry VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY unique_stock (stock_code, market)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,

            # 分笔数据表
            """
            CREATE TABLE IF NOT EXISTS tick_data (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                stock_code VARCHAR(20) NOT NULL,
                trade_time DATETIME NOT NULL,
                price DECIMAL(10,3) NOT NULL,
                price_change DECIMAL(10,3),
                volume INT NOT NULL,
                amount DECIMAL(15,2) NOT NULL,
                trade_type VARCHAR(10) NOT NULL,
                trade_date DATE NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_stock_date (stock_code, trade_date),
                INDEX idx_trade_time (trade_time)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,

            # 基础数据表
            """
            CREATE TABLE IF NOT EXISTS basic_data (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                stock_code VARCHAR(20) NOT NULL,
                trade_date DATE NOT NULL,
                period_type VARCHAR(20) NOT NULL,
                open_price DECIMAL(10,3),
                close_price DECIMAL(10,3),
                high_price DECIMAL(10,3),
                low_price DECIMAL(10,3),
                volume BIGINT,
                amount DECIMAL(15,2),
                change_price DECIMAL(10,3),
                change_pct DECIMAL(10,4),
                turnover_rate DECIMAL(10,4),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY unique_data (stock_code, trade_date, period_type)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,

            # 指标数据表
            """
            CREATE TABLE IF NOT EXISTS indicator_data (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                stock_code VARCHAR(20) NOT NULL,
                trade_date DATE NOT NULL,
                period_type VARCHAR(20) NOT NULL,
                indicator_name VARCHAR(50) NOT NULL,
                indicator_value DECIMAL(15,6),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                INDEX idx_stock_indicator (stock_code, indicator_name, trade_date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
        ]

        try:
            with self.engine.connect() as conn:
                for sql in create_tables_sql:
                    conn.execute(text(sql))
                conn.commit()
            logger.info("数据库表创建成功")
        except Exception as e:
            logger.error(f"创建数据库表失败: {e}")

    @contextmanager
    def get_session(self):
        """获取数据库会话的上下文管理器"""
        session = self.Session()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"数据库操作失败: {e}")
            raise
        finally:
            session.close()

    def execute_sql(self, sql, params=None):
        """执行SQL语句"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(sql), params or {})
                conn.commit()
                return result
        except Exception as e:
            logger.error(f"SQL执行失败: {sql}, 错误: {e}")
            raise

    def query_to_dataframe(self, sql, params=None):
        """执行查询并返回DataFrame"""
        try:
            # 使用 SQLAlchemy 的 text() 包装 SQL，支持命名参数
            with self.engine.connect() as conn:
                result = conn.execute(text(sql), params or {})
                # 将结果转换为 DataFrame
                df = pd.DataFrame(result.fetchall(), columns=result.keys())
                return df
        except Exception as e:
            logger.error(f"查询失败: {sql}, 错误: {e}")
            return pd.DataFrame()

    def insert_dataframe(self, df, table_name, if_exists='append'):
        """将DataFrame插入数据库"""
        try:
            df.to_sql(table_name, self.engine, if_exists=if_exists, index=False)
            logger.info(f"成功插入 {len(df)} 条数据到表 {table_name}")
        except Exception as e:
            logger.error(f"插入数据失败: {e}")
            raise

    def close(self):
        """关闭数据库连接"""
        if self.engine:
            self.engine.dispose()
            logger.info("数据库连接已关闭")


# 全局数据库实例
db_manager = DatabaseManager()
