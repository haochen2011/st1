"""
A股股票数据管理系统
重新组织的包结构，提供清晰的模块分工
"""

# 核心模块
from .core.config import config
from .core.models import init_database

# 数据模块
from .data.database import db_manager
from .data.enhanced_database import enhanced_db_manager

# 处理器模块
from .processors.batch_processor import batch_processor

# 导出模块
from .export.enhanced_excel_exporter import enhanced_excel_exporter

# 工具模块
from .utils.stock_info import stock_info
from .utils.stock_data_manager import StockDataManager

__version__ = "2.0.0"
__author__ = "Stock Analysis Team"

__all__ = [
    'config',
    'init_database',
    'db_manager',
    'enhanced_db_manager',
    'batch_processor',
    'enhanced_excel_exporter',
    'stock_info',
    'StockDataManager'
]
