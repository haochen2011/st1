"""
数据库模型定义和初始化模块
"""

from database import db_manager
from loguru import logger


def init_database():
    """初始化数据库"""
    try:
        # 这里可以创建必要的表结构
        logger.info("数据库初始化完成")
        return True
    except Exception as e:
        logger.error(f"数据库初始化失败: {e}")
        return False


def get_db_session():
    """获取数据库会话"""
    return db_manager


# 兼容性别名
SessionLocal = get_db_session