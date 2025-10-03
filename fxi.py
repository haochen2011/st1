"""
数据库结构修复工具
修复股票数据库中的字段缺失和字符集问题
"""
import sys
import os
from pathlib import Path

# 添加当前目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from loguru import logger
from data.enhanced_database import enhanced_db_manager
from sqlalchemy import text

class DatabaseFixer:
    """数据库修复器"""

    def __init__(self):
        self.db_manager = enhanced_db_manager

    def fix_all_issues(self):
        """修复所有数据库问题"""
        logger.info("开始修复数据库结构问题...")

        try:
            # 1. 修复表结构
            self.fix_table_structures()

            # 2. 修复字符集问题
            self.fix_charset_issues()

            # 3. 验证修复结果
            self.verify_fixes()

            # 4. 创建示例数据（如果需要）
            if input("是否创建示例数据用于测试? (y/N): ").strip().lower() in ['y', 'yes']:
                self.create_sample_data()

            logger.info("数据库修复完成！")
            return True

        except Exception as e:
            logger.error(f"数据库修复失败: {e}")
            return False

    def fix_table_structures(self):
        """修复表结构问题"""
        logger.info("修复表结构...")

        with self.db_manager.engine.connect() as conn:
            # 修复stock_info表
            self._fix_stock_info_structure(conn)

            # 修复indicator_data表
            self._fix_indicator_data_structure(conn)

            # 修复或创建basic_data表
            self._fix_basic_data_structure(conn)

            # 修复动态tick表
            self._fix_tick_tables_structure(conn)

            conn.commit()

    def _fix_stock_info_structure(self, conn):
        """修复stock_info表结构"""
        logger.info("检查stock_info表结构...")

        # 获取现有字段
        columns_result = conn.execute(text("SHOW COLUMNS FROM stock_info")).fetchall()
        existing_columns = [col[0] for col in columns_result]

        # 需要的字段定义
        required_fields = {
            'total_shares': 'BIGINT DEFAULT NULL COMMENT "总股本"',
            'float_shares': 'BIGINT DEFAULT NULL COMMENT "流通股本"',
            'industry': 'VARCHAR(100) DEFAULT NULL COMMENT "所属行业"'
        }

        # 添加缺失的字段
        for field_name, field_def in required_fields.items():
            if field_name not in existing_columns:
                alter_sql = f"ALTER TABLE stock_info ADD COLUMN {field_name} {field_def}"
                conn.execute(text(alter_sql))
                logger.info(f"添加字段: stock_info.{field_name}")

    def _fix_indicator_data_structure(self, conn):
        """修复indicator_data表结构"""
        logger.info("检查indicator_data表结构...")

        # 检查表是否存在
        table_check = conn.execute(text("SHOW TABLES LIKE 'indicator_data'")).fetchone()
        if not table_check:
            logger.info("创建indicator_data表...")
            create_sql = """
            CREATE TABLE indicator_data (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                stock_code VARCHAR(10) NOT NULL COMMENT '股票代码',
                period VARCHAR(20) NOT NULL COMMENT '周期',
                trade_date DATE NOT NULL COMMENT '交易日期',
                indicator_name VARCHAR(50) NOT NULL COMMENT '指标名称',
                indicator_value DECIMAL(15,6) DEFAULT NULL COMMENT '指标值',
                indicator_data JSON DEFAULT NULL COMMENT '指标详细数据',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY uk_stock_period_date_indicator (stock_code, period, trade_date, indicator_name),
                INDEX idx_stock_code (stock_code),
                INDEX idx_indicator_name (indicator_name),
                INDEX idx_trade_date (trade_date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """
            conn.execute(text(create_sql))
            logger.info("indicator_data表创建完成")
            return

        # 获取现有字段
        columns_result = conn.execute(text("SHOW COLUMNS FROM indicator_data")).fetchall()
        existing_columns = [col[0] for col in columns_result]

        # 需要的字段
        required_fields = {
            'indicator_value': 'DECIMAL(15,6) DEFAULT NULL COMMENT "指标值"',
            'trade_date': 'DATE NOT NULL COMMENT "交易日期"'
        }

        # 添加缺失的字段
        for field_name, field_def in required_fields.items():
            if field_name not in existing_columns:
                alter_sql = f"ALTER TABLE indicator_data ADD COLUMN {field_name} {field_def}"
                conn.execute(text(alter_sql))
                logger.info(f"添加字段: indicator_data.{field_name}")

    def _fix_basic_data_structure(self, conn):
        """修复或创建basic_data分周期表结构"""
        logger.info("检查basic_data分周期表结构...")

        # 定义所有周期
        periods = ['1min', '5min', '10min', '15min', '30min', '1hour', 'daily', 'week', 'month', 'quarter', 'half_year', 'year']
        default_period = 'daily'

        # 首先检查是否存在旧的basic_data表
        old_table_check = conn.execute(text("SHOW TABLES LIKE 'basic_data'")).fetchone()
        if old_table_check:
            logger.info("发现旧的basic_data表，将重命名为basic_data_daily")
            try:
                # 重命名旧表为daily周期表
                rename_sql = "RENAME TABLE basic_data TO basic_data_daily"
                conn.execute(text(rename_sql))
                logger.info("旧表已重命名为basic_data_daily")
            except Exception as e:
                logger.warning(f"重命名旧表失败: {e}")

        # 为每个周期创建或修复表
        for period in periods:
            table_name = f"basic_data_{period}"
            self._create_or_fix_basic_data_table(conn, table_name, period)

    def _create_or_fix_basic_data_table(self, conn, table_name, period):
        """创建或修复单个basic_data表"""
        try:
            # 检查表是否存在
            table_check = conn.execute(text(f"SHOW TABLES LIKE '{table_name}'")).fetchone()

            if not table_check:
                logger.info(f"创建{table_name}表...")
                create_sql = f"""
                CREATE TABLE {table_name} (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    stock_code VARCHAR(10) NOT NULL COMMENT '股票代码',
                    period VARCHAR(20) NOT NULL DEFAULT '{period}' COMMENT '周期',
                    trade_date DATE NOT NULL COMMENT '交易日期',
                    trade_time DATETIME DEFAULT NULL COMMENT '交易时间',
                    open_price DECIMAL(10,3) DEFAULT NULL COMMENT '开盘价',
                    high_price DECIMAL(10,3) DEFAULT NULL COMMENT '最高价',
                    low_price DECIMAL(10,3) DEFAULT NULL COMMENT '最低价',
                    close_price DECIMAL(10,3) DEFAULT NULL COMMENT '收盘价',
                    volume BIGINT DEFAULT NULL COMMENT '成交量',
                    amount DECIMAL(20,2) DEFAULT NULL COMMENT '成交金额',
                    turnover_rate DECIMAL(8,4) DEFAULT NULL COMMENT '换手率',
                    pe_ratio DECIMAL(10,2) DEFAULT NULL COMMENT '市盈率',
                    pb_ratio DECIMAL(10,2) DEFAULT NULL COMMENT '市净率',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    UNIQUE KEY uk_stock_date_time (stock_code, trade_date, trade_time),
                    INDEX idx_stock_code (stock_code),
                    INDEX idx_trade_date (trade_date),
                    INDEX idx_period (period)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
                conn.execute(text(create_sql))
                logger.info(f"{table_name}表创建完成")
                return

            # 如果表存在，检查和修复字段
            columns_result = conn.execute(text(f"SHOW COLUMNS FROM {table_name}")).fetchall()
            existing_columns = [col[0] for col in columns_result]

            # 需要的字段
            required_fields = {
                'trade_date': 'DATE NOT NULL COMMENT "交易日期"',
                'period': f'VARCHAR(20) NOT NULL DEFAULT "{period}" COMMENT "周期"',
                'amount': 'DECIMAL(20,2) DEFAULT NULL COMMENT "成交金额"',
                'trade_time': 'DATETIME DEFAULT NULL COMMENT "交易时间"'
            }

            # 添加缺失的字段
            for field_name, field_def in required_fields.items():
                if field_name not in existing_columns:
                    alter_sql = f"ALTER TABLE {table_name} ADD COLUMN {field_name} {field_def}"
                    conn.execute(text(alter_sql))
                    logger.info(f"添加字段: {table_name}.{field_name}")

        except Exception as e:
            logger.error(f"创建或修复{table_name}表失败: {e}")

    def _fix_tick_tables_structure(self, conn):
        """修复动态tick表结构"""
        try:
            # 获取所有tick表
            tick_tables_result = conn.execute(text("SHOW TABLES LIKE 'tick_data_%'")).fetchall()

            for table in tick_tables_result:
                table_name = table[0]
                try:
                    # 检查tick表的字段
                    columns_result = conn.execute(text(f"SHOW COLUMNS FROM {table_name}")).fetchall()
                    existing_columns = [col[0] for col in columns_result]

                    # 确保必要字段存在
                    required_fields = {
                        'trade_date': 'DATE DEFAULT NULL COMMENT "交易日期"'
                    }

                    for field_name, field_def in required_fields.items():
                        if field_name not in existing_columns:
                            alter_sql = f"ALTER TABLE {table_name} ADD COLUMN {field_name} {field_def}"
                            conn.execute(text(alter_sql))
                            logger.info(f"添加字段: {table_name}.{field_name}")

                except Exception as e:
                    logger.warning(f"修复 {table_name} 表结构失败: {e}")

        except Exception as e:
            logger.warning(f"修复tick表结构失败: {e}")

    def fix_charset_issues(self):
        """修复字符集问题"""
        logger.info("修复字符集问题...")

        with self.db_manager.engine.connect() as conn:
            # 首先设置连接字符集
            conn.execute(text("SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci"))

            # 修复主要表的字符集
            tables_to_fix = ['stock_info', 'indicator_data', 'basic_data']

            for table_name in tables_to_fix:
                try:
                    # 检查表是否存在
                    table_check = conn.execute(text(f"SHOW TABLES LIKE '{table_name}'")).fetchone()
                    if table_check:
                        # 先修复表的默认字符集
                        alter_table_sql = f"ALTER TABLE {table_name} DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
                        conn.execute(text(alter_table_sql))

                        # 再转换表内容的字符集
                        convert_sql = f"ALTER TABLE {table_name} CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
                        conn.execute(text(convert_sql))

                        logger.info(f"修复 {table_name} 表字符集")
                except Exception as e:
                    logger.warning(f"修复 {table_name} 表字符集失败: {e}")

            # 修复动态tick表的字符集
            self._fix_tick_tables_charset_enhanced(conn)

            conn.commit()

    def _fix_tick_tables_charset_enhanced(self, conn):
        """增强的tick表字符集修复"""
        try:
            # 获取所有tick表
            tick_tables_result = conn.execute(text("SHOW TABLES LIKE 'tick_data_%'")).fetchall()

            for table in tick_tables_result:
                table_name = table[0]
                try:
                    # 先修复表的默认字符集
                    alter_table_sql = f"ALTER TABLE {table_name} DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
                    conn.execute(text(alter_table_sql))

                    # 再转换表内容的字符集
                    convert_sql = f"ALTER TABLE {table_name} CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
                    conn.execute(text(convert_sql))

                    logger.info(f"修复 {table_name} 表字符集")
                except Exception as e:
                    logger.warning(f"修复 {table_name} 表字符集失败: {e}")

        except Exception as e:
            logger.warning(f"修复tick表字符集失败: {e}")

    def verify_fixes(self):
        """验证修复结果"""
        logger.info("验证修复结果...")

        with self.db_manager.engine.connect() as conn:
            # 验证stock_info表
            self._verify_stock_info(conn)

            # 验证indicator_data表
            self._verify_indicator_data(conn)

            # 验证basic_data表
            self._verify_basic_data(conn)

    def _verify_stock_info(self, conn):
        """验证stock_info表"""
        try:
            columns_result = conn.execute(text("SHOW COLUMNS FROM stock_info")).fetchall()
            existing_columns = [col[0] for col in columns_result]

            required_fields = ['total_shares', 'float_shares', 'industry']
            missing_fields = [field for field in required_fields if field not in existing_columns]

            if missing_fields:
                logger.warning(f"stock_info表仍缺少字段: {missing_fields}")
            else:
                logger.info("stock_info表结构验证通过")

        except Exception as e:
            logger.error(f"验证stock_info表失败: {e}")

    def _verify_indicator_data(self, conn):
        """验证indicator_data表"""
        try:
            table_check = conn.execute(text("SHOW TABLES LIKE 'indicator_data'")).fetchone()
            if not table_check:
                logger.warning("indicator_data表不存在")
                return

            columns_result = conn.execute(text("SHOW COLUMNS FROM indicator_data")).fetchall()
            existing_columns = [col[0] for col in columns_result]

            required_fields = ['indicator_value', 'trade_date']
            missing_fields = [field for field in required_fields if field not in existing_columns]

            if missing_fields:
                logger.warning(f"indicator_data表仍缺少字段: {missing_fields}")
            else:
                logger.info("indicator_data表结构验证通过")

        except Exception as e:
            logger.error(f"验证indicator_data表失败: {e}")

    def _verify_basic_data(self, conn):
        """验证basic_data分周期表"""
        try:
            periods = ['daily', '1min', '5min']  # 验证主要的几个周期

            for period in periods:
                table_name = f"basic_data_{period}"
                table_check = conn.execute(text(f"SHOW TABLES LIKE '{table_name}'")).fetchone()

                if table_check:
                    columns_result = conn.execute(text(f"SHOW COLUMNS FROM {table_name}")).fetchall()
                    existing_columns = [col[0] for col in columns_result]

                    required_fields = ['trade_date', 'period', 'amount']
                    missing_fields = [field for field in required_fields if field not in existing_columns]

                    if missing_fields:
                        logger.warning(f"{table_name}表仍缺少字段: {missing_fields}")
                    else:
                        logger.info(f"{table_name}表结构验证通过")
                else:
                    logger.info(f"{table_name}表不存在（这是正常的，除非有数据）")

        except Exception as e:
            logger.error(f"验证basic_data表失败: {e}")

    def create_sample_data(self):
        """创建示例数据，用于测试"""
        logger.info("创建示例数据...")

        try:
            with self.db_manager.engine.connect() as conn:
                # 为daily表创建示例数据
                daily_table = "basic_data_daily"

                # 检查daily表是否存在
                table_check = conn.execute(text(f"SHOW TABLES LIKE '{daily_table}'")).fetchone()
                if not table_check:
                    logger.warning(f"{daily_table}表不存在，跳过创建示例数据")
                    return

                # 检查表是否为空
                count_result = conn.execute(text(f"SELECT COUNT(*) FROM {daily_table}")).fetchone()
                if count_result[0] == 0:
                    logger.info(f"{daily_table}表为空，创建示例数据...")

                    # 插入一些示例数据
                    sample_data_sql = f"""
                    INSERT INTO {daily_table} (stock_code, trade_date, period, open_price, close_price, volume, amount)
                    VALUES
                    ('000001', CURDATE(), 'daily', 10.50, 10.60, 1000000, 10600000.00),
                    ('000002', CURDATE(), 'daily', 15.20, 15.35, 800000, 12280000.00),
                    ('600000', CURDATE(), 'daily', 8.80, 8.95, 1200000, 10740000.00),
                    ('000858', CURDATE(), 'daily', 25.30, 25.45, 500000, 12725000.00),
                    ('002594', CURDATE(), 'daily', 18.60, 18.75, 750000, 14062500.00)
                    ON DUPLICATE KEY UPDATE updated_at = CURRENT_TIMESTAMP
                    """
                    conn.execute(text(sample_data_sql))
                    logger.info("示例数据创建完成")
                else:
                    logger.info(f"{daily_table}表已有数据，跳过创建示例数据")

                conn.commit()

        except Exception as e:
            logger.warning(f"创建示例数据失败: {e}")

def main():
    """主函数"""
    logger.info("=" * 60)
    logger.info("股票数据库修复工具")
    logger.info("=" * 60)

    fixer = DatabaseFixer()

    try:
        success = fixer.fix_all_issues()

        if success:
            print("\n✅ 数据库修复成功！")
            print("现在可以重新运行启动.py了")
        else:
            print("\n❌ 数据库修复失败！")
            print("请检查日志信息并手动修复")

    except Exception as e:
        logger.error(f"修复过程中发生错误: {e}")
        print(f"\n❌ 修复过程中发生错误: {e}")

if __name__ == "__main__":
    main()