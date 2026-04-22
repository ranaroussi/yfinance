#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
股票监控应用主入口
基于 yfinance 和 PyQt5 开发
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import desktop_tools

from PyQt5.QtWidgets import QApplication, QMessageBox
from PyQt5.QtGui import QFont
from PyQt5.QtCore import Qt

YFINANCE_AVAILABLE = False
PYQT_AVAILABLE = False


def check_dependencies():
    global YFINANCE_AVAILABLE, PYQT_AVAILABLE

    try:
        from PyQt5.QtWidgets import QApplication
        PYQT_AVAILABLE = True
    except ImportError:
        PYQT_AVAILABLE = False

    try:
        import yfinance
        YFINANCE_AVAILABLE = True
    except ImportError:
        YFINANCE_AVAILABLE = False
    except TypeError as e:
        if "'type' object is not subscriptable" in str(e):
            print(f"\n错误: multitasking 库与 Python 3.8 不兼容。")
            print(f"\n解决方案:")
            print(f"  1. 升级 Python 到 3.9 或更高版本")
            print(f"  2. 或者降级 multitasking 到兼容版本:")
            print(f"     pip install multitasking==0.0.11")
            YFINANCE_AVAILABLE = False
        else:
            raise

    missing = []
    if not YFINANCE_AVAILABLE:
        missing.append("yfinance")
    if not PYQT_AVAILABLE:
        missing.append("PyQt5")

    if missing:
        print(f"错误: 缺少必要的依赖包: {', '.join(missing)}")
        print("\n请运行以下命令安装依赖:")
        print("  pip install PyQt5 yfinance")
        return False

    return True


def main():
    if not check_dependencies():
        input("\n按回车键退出...")
        sys.exit(1)

    from desktop_tools.gui import MainWindow
    from desktop_tools.config import ConfigManager
    from desktop_tools.watchlist import WatchlistManager

    app = QApplication(sys.argv)

    app.setStyle('Fusion')
    font = QFont("Microsoft YaHei", 9)
    app.setFont(font)

    app.setAttribute(Qt.AA_EnableHighDpiScaling)
    app.setAttribute(Qt.AA_UseHighDpiPixmaps)

    config = ConfigManager()
    watchlist = WatchlistManager()
    watchlist.refresh_all()

    window = MainWindow()
    window.show()

    sys.exit(app.exec_())


if __name__ == "__main__":
    main()
