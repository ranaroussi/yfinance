#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
股票监控应用启动脚本
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

if __name__ == "__main__":
    from desktop_tools.main import main
    main()
