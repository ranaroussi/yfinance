#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
股票监控应用主入口
基于 yfinance 和 PyQt5 开发
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from PyQt5.QtWidgets import QApplication, QMessageBox
from PyQt5.QtGui import QFont
from PyQt5.QtCore import Qt

try:
    import yfinance
    YFINANCE_AVAILABLE = True
except ImportError:
    YFINANCE_AVAILABLE = False

try:
    from PyQt5.QtWidgets import QApplication
    PYQT_AVAILABLE = True
except ImportError:
    PYQT_AVAILABLE = False


def check_dependencies():
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
