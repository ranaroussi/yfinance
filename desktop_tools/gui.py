from PyQt5.QtWidgets import (
    QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QTabWidget, QLabel, QPushButton, QTableWidget,
    QTableWidgetItem, QHeaderView, QLineEdit, QGroupBox,
    QSpinBox, QCheckBox, QComboBox, QProgressBar, QTextEdit,
    QSplitter, QMessageBox, QStatusBar, QToolBar, QAction,
    QMenu, QMenuBar, QFrame, QSizePolicy
)
from PyQt5.QtCore import Qt, QTimer, pyqtSignal, QThread, QUrl
from PyQt5.QtGui import QFont, QColor, QDesktopServices, QIcon
import sys
from typing import List, Dict, Any, Optional
from datetime import datetime

from .config import ConfigManager
from .watchlist import WatchlistManager
from .data_provider import DataProvider
from .news_manager import NewsManager
from .screener import StockScreener


class WatchlistTab(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._config = ConfigManager()
        self._watchlist_manager = WatchlistManager()
        self._data_provider = DataProvider()
        self._init_ui()
        self._init_connections()
        self._refresh_data()

    def _init_ui(self):
        layout = QVBoxLayout(self)
        layout.setSpacing(10)

        control_group = QGroupBox("操作")
        control_layout = QHBoxLayout(control_group)

        control_layout.addWidget(QLabel("添加股票:"))
        self._symbol_input = QLineEdit()
        self._symbol_input.setPlaceholderText("输入股票代码，如 AAPL")
        self._symbol_input.setMaximumWidth(150)
        control_layout.addWidget(self._symbol_input)

        self._add_btn = QPushButton("添加")
        control_layout.addWidget(self._add_btn)

        control_layout.addSpacing(20)

        self._remove_btn = QPushButton("移除选中")
        control_layout.addWidget(self._remove_btn)

        self._refresh_btn = QPushButton("刷新数据")
        control_layout.addWidget(self._refresh_btn)

        control_layout.addSpacing(20)

        control_layout.addWidget(QLabel("排序:"))
        self._sort_combo = QComboBox()
        self._sort_combo.addItems([
            "股票代码", "名称", "当前价格", "涨跌幅",
            "成交量", "市值"
        ])
        control_layout.addWidget(self._sort_combo)

        self._sort_order_check = QCheckBox("降序")
        self._sort_order_check.setChecked(False)
        control_layout.addWidget(self._sort_order_check)

        control_layout.addStretch()

        layout.addWidget(control_group)

        stats_group = QGroupBox("统计信息")
        stats_layout = QHBoxLayout(stats_group)

        self._total_label = QLabel("股票总数: 0")
        stats_layout.addWidget(self._total_label)

        stats_layout.addSpacing(20)

        self._up_label = QLabel("上涨: 0")
        self._up_label.setStyleSheet("color: green; font-weight: bold;")
        stats_layout.addWidget(self._up_label)

        self._down_label = QLabel("下跌: 0")
        self._down_label.setStyleSheet("color: red; font-weight: bold;")
        stats_layout.addWidget(self._down_label)

        stats_layout.addSpacing(20)

        self._avg_change_label = QLabel("平均涨跌幅: 0.00%")
        stats_layout.addWidget(self._avg_change_label)

        self._last_update_label = QLabel("最后更新: --")
        stats_layout.addStretch()
        stats_layout.addWidget(self._last_update_label)

        layout.addWidget(stats_group)

        self._table = QTableWidget()
        self._table.setColumnCount(11)
        self._table.setHorizontalHeaderLabels([
            "股票代码", "名称", "当前价格", "涨跌", "涨跌幅",
            "开盘价", "最高价", "最低价", "成交量", "市值", "货币"
        ])
        self._table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self._table.setSelectionBehavior(QTableWidget.SelectRows)
        self._table.setAlternatingRowColors(True)
        self._table.setEditTriggers(QTableWidget.NoEditTriggers)

        layout.addWidget(self._table)

    def _init_connections(self):
        self._add_btn.clicked.connect(self._add_stock)
        self._symbol_input.returnPressed.connect(self._add_stock)
        self._remove_btn.clicked.connect(self._remove_selected)
        self._refresh_btn.clicked.connect(self._refresh_data)
        self._sort_combo.currentIndexChanged.connect(self._sort_table)
        self._sort_order_check.stateChanged.connect(self._sort_table)
        self._watchlist_manager.add_update_callback(self._on_data_updated)

    def _add_stock(self):
        symbol = self._symbol_input.text().strip().upper()
        if not symbol:
            QMessageBox.warning(self, "提示", "请输入股票代码")
            return

        if self._watchlist_manager.is_in_watchlist(symbol):
            QMessageBox.warning(self, "提示", f"股票 {symbol} 已在自选股中")
            return

        if self._watchlist_manager.add_stock(symbol):
            self._symbol_input.clear()
            QMessageBox.information(self, "成功", f"已添加股票 {symbol}")
        else:
            QMessageBox.warning(self, "失败", f"添加股票 {symbol} 失败")

    def _remove_selected(self):
        selected = self._table.selectedItems()
        if not selected:
            QMessageBox.warning(self, "提示", "请先选择要移除的股票")
            return

        rows = set(item.row() for item in selected)
        symbols_to_remove = []
        for row in rows:
            symbol_item = self._table.item(row, 0)
            if symbol_item:
                symbols_to_remove.append(symbol_item.text())

        if symbols_to_remove:
            reply = QMessageBox.question(
                self, "确认",
                f"确定要移除以下股票吗？\n{', '.join(symbols_to_remove)}",
                QMessageBox.Yes | QMessageBox.No
            )
            if reply == QMessageBox.Yes:
                for symbol in symbols_to_remove:
                    self._watchlist_manager.remove_stock(symbol)

    def _refresh_data(self):
        self._watchlist_manager.refresh_all()

    def _on_data_updated(self):
        self._update_table()
        self._update_stats()

    def _update_table(self):
        sort_field_map = {
            "股票代码": "symbol",
            "名称": "name",
            "当前价格": "current_price",
            "涨跌幅": "change_percent",
            "成交量": "volume",
            "市值": "market_cap"
        }
        sort_by = sort_field_map.get(self._sort_combo.currentText(), "symbol")
        ascending = not self._sort_order_check.isChecked()

        quotes = self._watchlist_manager.get_sorted_quotes(sort_by=sort_by, ascending=ascending)

        self._table.setRowCount(len(quotes))

        for row, quote in enumerate(quotes):
            self._table.setItem(row, 0, QTableWidgetItem(quote.get('symbol', '')))
            self._table.setItem(row, 1, QTableWidgetItem(quote.get('name', '')))

            current_price = quote.get('current_price', 0)
            current_item = QTableWidgetItem(f"{current_price:.2f}" if current_price else "--")
            self._table.setItem(row, 2, current_item)

            change = quote.get('change', 0)
            change_pct = quote.get('change_percent', 0)

            change_item = QTableWidgetItem(f"{change:+.2f}" if change else "--")
            change_pct_item = QTableWidgetItem(f"{change_pct:+.2f}%" if change_pct else "--")

            if change_pct > 0:
                color = QColor(0, 150, 0)
            elif change_pct < 0:
                color = QColor(200, 0, 0)
            else:
                color = QColor(0, 0, 0)

            change_item.setForeground(color)
            change_pct_item.setForeground(color)
            current_item.setForeground(color)

            self._table.setItem(row, 3, change_item)
            self._table.setItem(row, 4, change_pct_item)

            open_price = quote.get('open', 0)
            high = quote.get('high', 0)
            low = quote.get('low', 0)
            volume = quote.get('volume', 0)
            market_cap = quote.get('market_cap', 0)

            self._table.setItem(row, 5, QTableWidgetItem(f"{open_price:.2f}" if open_price else "--"))
            self._table.setItem(row, 6, QTableWidgetItem(f"{high:.2f}" if high else "--"))
            self._table.setItem(row, 7, QTableWidgetItem(f"{low:.2f}" if low else "--"))
            self._table.setItem(row, 8, QTableWidgetItem(self._format_number(volume)))
            self._table.setItem(row, 9, QTableWidgetItem(self._format_market_cap(market_cap)))
            self._table.setItem(row, 10, QTableWidgetItem(quote.get('currency', '')))

    def _update_stats(self):
        stats = self._watchlist_manager.get_total_value()

        self._total_label.setText(f"股票总数: {stats['total_stocks']}")
        self._up_label.setText(f"上涨: {stats['up_count']}")
        self._down_label.setText(f"下跌: {stats['down_count']}")

        avg_change = stats['avg_change_percent']
        self._avg_change_label.setText(f"平均涨跌幅: {avg_change:+.2f}%")

        last_update = self._watchlist_manager.last_update
        if last_update:
            self._last_update_label.setText(f"最后更新: {last_update.strftime('%H:%M:%S')}")

    def _sort_table(self):
        self._update_table()

    def _format_number(self, num: float) -> str:
        if num >= 1_000_000:
            return f"{num/1_000_000:.2f}M"
        elif num >= 1_000:
            return f"{num/1_000:.2f}K"
        return f"{num:.0f}" if num else "--"

    def _format_market_cap(self, cap: float) -> str:
        if cap >= 1_000_000_000_000:
            return f"{cap/1_000_000_000_000:.2f}T"
        elif cap >= 1_000_000_000:
            return f"{cap/1_000_000_000:.2f}B"
        elif cap >= 1_000_000:
            return f"{cap/1_000_000:.2f}M"
        return f"{cap:.0f}" if cap else "--"

    def refresh(self):
        self._refresh_data()


class NewsTab(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._config = ConfigManager()
        self._news_manager = NewsManager()
        self._watchlist_manager = WatchlistManager()
        self._init_ui()
        self._init_connections()

    def _init_ui(self):
        layout = QVBoxLayout(self)
        layout.setSpacing(10)

        control_group = QGroupBox("新闻设置")
        control_layout = QHBoxLayout(control_group)

        control_layout.addWidget(QLabel("查看股票:"))
        self._symbol_combo = QComboBox()
        self._symbol_combo.setEditable(True)
        self._symbol_combo.setMaximumWidth(150)
        control_layout.addWidget(self._symbol_combo)

        control_layout.addWidget(QLabel("新闻数量:"))
        self._count_spin = QSpinBox()
        self._count_spin.setRange(1, 50)
        self._count_spin.setValue(self._config.news_count)
        control_layout.addWidget(self._count_spin)

        self._fetch_btn = QPushButton("获取新闻")
        control_layout.addWidget(self._fetch_btn)

        self._fetch_all_btn = QPushButton("获取所有自选股新闻")
        control_layout.addWidget(self._fetch_all_btn)

        control_layout.addStretch()

        layout.addWidget(control_group)

        self._progress_bar = QProgressBar()
        self._progress_bar.setVisible(False)
        layout.addWidget(self._progress_bar)

        self._news_table = QTableWidget()
        self._news_table.setColumnCount(4)
        self._news_table.setHorizontalHeaderLabels([
            "股票代码", "标题", "发布时间", "来源"
        ])
        self._news_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.Fixed)
        self._news_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
        self._news_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.Fixed)
        self._news_table.horizontalHeader().setSectionResizeMode(3, QHeaderView.Fixed)
        self._news_table.setColumnWidth(0, 80)
        self._news_table.setColumnWidth(2, 150)
        self._news_table.setColumnWidth(3, 100)
        self._news_table.setSelectionBehavior(QTableWidget.SelectRows)
        self._news_table.setAlternatingRowColors(True)
        self._news_table.setEditTriggers(QTableWidget.NoEditTriggers)
        self._news_table.setSelectionMode(QTableWidget.SingleSelection)

        layout.addWidget(self._news_table)

        detail_group = QGroupBox("新闻详情")
        detail_layout = QVBoxLayout(detail_group)

        self._title_label = QLabel("标题: --")
        self._title_label.setWordWrap(True)
        self._title_label.setFont(QFont("", 10, QFont.Bold))
        detail_layout.addWidget(self._title_label)

        self._publisher_label = QLabel("来源: --")
        detail_layout.addWidget(self._publisher_label)

        self._date_label = QLabel("发布时间: --")
        detail_layout.addWidget(self._date_label)

        link_layout = QHBoxLayout()
        self._link_label = QLabel("链接: --")
        self._link_label.setOpenExternalLinks(True)
        link_layout.addWidget(self._link_label)

        self._open_link_btn = QPushButton("在浏览器中打开")
        self._open_link_btn.setEnabled(False)
        link_layout.addWidget(self._open_link_btn)

        detail_layout.addLayout(link_layout)
        layout.addWidget(detail_group)

        self._update_symbol_combo()

    def _init_connections(self):
        self._fetch_btn.clicked.connect(self._fetch_news)
        self._fetch_all_btn.clicked.connect(self._fetch_all_news)
        self._news_table.itemSelectionChanged.connect(self._on_news_selected)
        self._open_link_btn.clicked.connect(self._open_news_link)
        self._watchlist_manager.add_update_callback(self._update_symbol_combo)

    def _update_symbol_combo(self):
        current_text = self._symbol_combo.currentText()
        self._symbol_combo.clear()
        self._symbol_combo.addItems(self._watchlist_manager.watchlist)
        if current_text:
            index = self._symbol_combo.findText(current_text)
            if index >= 0:
                self._symbol_combo.setCurrentIndex(index)

    def _fetch_news(self):
        symbol = self._symbol_combo.currentText().strip().upper()
        if not symbol:
            QMessageBox.warning(self, "提示", "请输入或选择股票代码")
            return

        count = self._count_spin.value()
        news_list = self._news_manager.get_news_for_symbol(symbol, count, force_refresh=True)
        self._display_news(news_list)

    def _fetch_all_news(self):
        watchlist = self._watchlist_manager.watchlist
        if not watchlist:
            QMessageBox.warning(self, "提示", "自选股列表为空，请先添加股票")
            return

        self._progress_bar.setVisible(True)
        self._progress_bar.setRange(0, len(watchlist))
        self._progress_bar.setValue(0)

        def progress_callback(current, total, symbol):
            self._progress_bar.setValue(current)

        news_list = self._news_manager.get_all_watchlist_news(
            self._count_spin.value(),
            progress_callback
        )

        self._progress_bar.setVisible(False)
        self._display_news(news_list)

    def _display_news(self, news_list: List[Dict[str, Any]]):
        self._news_table.setRowCount(len(news_list))

        for row, news in enumerate(news_list):
            self._news_table.setItem(row, 0, QTableWidgetItem(news.get('symbol', '')))

            title_item = QTableWidgetItem(news.get('title', ''))
            title_item.setData(Qt.UserRole, news)
            self._news_table.setItem(row, 1, title_item)

            self._news_table.setItem(row, 2, QTableWidgetItem(news.get('formatted_date', '')))
            self._news_table.setItem(row, 3, QTableWidgetItem(news.get('publisher', '')))

        self._current_news = None
        self._open_link_btn.setEnabled(False)

    def _on_news_selected(self):
        selected = self._news_table.selectedItems()
        if not selected:
            return

        row = selected[0].row()
        title_item = self._news_table.item(row, 1)
        if not title_item:
            return

        news = title_item.data(Qt.UserRole)
        if not news:
            return

        self._current_news = news

        self._title_label.setText(f"标题: {news.get('title', '--')}")
        self._publisher_label.setText(f"来源: {news.get('publisher', '--')}")
        self._date_label.setText(f"发布时间: {news.get('formatted_date', '--')}")

        link = news.get('link', '')
        if link:
            self._link_label.setText(f'链接: <a href="{link}">{link}</a>')
            self._open_link_btn.setEnabled(True)
        else:
            self._link_label.setText("链接: --")
            self._open_link_btn.setEnabled(False)

    def _open_news_link(self):
        if self._current_news and 'link' in self._current_news:
            QDesktopServices.openUrl(QUrl(self._current_news['link']))

    def refresh(self):
        pass


class ScreenerThread(QThread):
    progress_signal = pyqtSignal(int, int, str)
    finished_signal = pyqtSignal(list)

    def __init__(self, screener_type: str, params: Dict[str, Any], parent=None):
        super().__init__(parent)
        self._screener_type = screener_type
        self._params = params
        self._screener = StockScreener()

    def run(self):
        results = []

        def progress_callback(current, total, symbol):
            self.progress_signal.emit(current, total, symbol)

        try:
            if self._screener_type == 'recommendation':
                min_buy_ratio = self._params.get('min_buy_ratio', 0.5)
                limit = self._params.get('limit', 50)
                results = self._screener.screen_by_recommendation(
                    min_buy_ratio=min_buy_ratio,
                    progress_callback=progress_callback,
                    limit=limit
                )
            elif self._screener_type == 'price_target':
                min_upside = self._params.get('min_upside', 10.0)
                limit = self._params.get('limit', 50)
                results = self._screener.screen_by_price_target(
                    min_upside=min_upside,
                    progress_callback=progress_callback,
                    limit=limit
                )
            elif self._screener_type == 'insider':
                days = self._params.get('days', 30)
                limit = self._params.get('limit', 50)
                results = self._screener.screen_by_insider_buys(
                    days=days,
                    progress_callback=progress_callback,
                    limit=limit
                )
            elif self._screener_type == 'piotroski':
                min_fscore = self._params.get('min_fscore', 7)
                limit = self._params.get('limit', 50)
                results = self._screener.screen_by_piotroski_fscore(
                    min_fscore=min_fscore,
                    progress_callback=progress_callback,
                    limit=limit
                )
        except Exception as e:
            print(f"选股出错: {e}")

        self.finished_signal.emit(results)


class ScreenerTab(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._screener = StockScreener()
        self._watchlist_manager = WatchlistManager()
        self._data_provider = DataProvider()
        self._screener_thread: Optional[ScreenerThread] = None
        self._init_ui()
        self._init_connections()

    def _init_ui(self):
        layout = QVBoxLayout(self)
        layout.setSpacing(10)

        screener_group = QGroupBox("选股工具")
        screener_layout = QVBoxLayout(screener_group)

        type_layout = QHBoxLayout()
        type_layout.addWidget(QLabel("选股类型:"))

        self._screener_combo = QComboBox()
        self._screener_combo.addItems([
            "推荐评级选股",
            "目标价选股",
            "内部人买卖选股",
            "皮奥特罗斯基选股"
        ])
        self._screener_combo.setMaximumWidth(200)
        type_layout.addWidget(self._screener_combo)
        type_layout.addStretch()

        screener_layout.addLayout(type_layout)

        self._param_widgets = QWidget()
        param_layout = QVBoxLayout(self._param_widgets)
        param_layout.setContentsMargins(0, 0, 0, 0)

        self._rec_widget = self._create_rec_params()
        self._target_widget = self._create_target_params()
        self._insider_widget = self._create_insider_params()
        self._piotroski_widget = self._create_piotroski_params()

        param_layout.addWidget(self._rec_widget)
        param_layout.addWidget(self._target_widget)
        param_layout.addWidget(self._insider_widget)
        param_layout.addWidget(self._piotroski_widget)

        screener_layout.addWidget(self._param_widgets)

        self._update_param_widgets()

        btn_layout = QHBoxLayout()
        self._run_btn = QPushButton("开始选股")
        self._run_btn.setMinimumHeight(40)
        btn_layout.addWidget(self._run_btn)

        self._add_to_watchlist_btn = QPushButton("添加选中到自选股")
        self._add_to_watchlist_btn.setEnabled(False)
        btn_layout.addWidget(self._add_to_watchlist_btn)

        btn_layout.addStretch()
        screener_layout.addLayout(btn_layout)

        layout.addWidget(screener_group)

        self._progress_label = QLabel("进度: 等待开始...")
        layout.addWidget(self._progress_label)

        self._progress_bar = QProgressBar()
        layout.addWidget(self._progress_bar)

        self._result_table = QTableWidget()
        self._result_table.setSelectionBehavior(QTableWidget.SelectRows)
        self._result_table.setAlternatingRowColors(True)
        self._result_table.setEditTriggers(QTableWidget.NoEditTriggers)

        layout.addWidget(self._result_table)

    def _create_rec_params(self) -> QWidget:
        widget = QWidget()
        layout = QHBoxLayout(widget)

        layout.addWidget(QLabel("最低买入比例:"))
        self._min_buy_ratio_spin = QSpinBox()
        self._min_buy_ratio_spin.setRange(10, 100)
        self._min_buy_ratio_spin.setValue(50)
        self._min_buy_ratio_spin.setSuffix("%")
        layout.addWidget(self._min_buy_ratio_spin)

        layout.addSpacing(20)
        layout.addWidget(QLabel("返回数量:"))
        self._rec_limit_spin = QSpinBox()
        self._rec_limit_spin.setRange(1, 250)
        self._rec_limit_spin.setValue(50)
        layout.addWidget(self._rec_limit_spin)

        layout.addStretch()
        return widget

    def _create_target_params(self) -> QWidget:
        widget = QWidget()
        layout = QHBoxLayout(widget)

        layout.addWidget(QLabel("最低上涨空间:"))
        self._min_upside_spin = QSpinBox()
        self._min_upside_spin.setRange(1, 200)
        self._min_upside_spin.setValue(10)
        self._min_upside_spin.setSuffix("%")
        layout.addWidget(self._min_upside_spin)

        layout.addSpacing(20)
        layout.addWidget(QLabel("返回数量:"))
        self._target_limit_spin = QSpinBox()
        self._target_limit_spin.setRange(1, 250)
        self._target_limit_spin.setValue(50)
        layout.addWidget(self._target_limit_spin)

        layout.addStretch()
        return widget

    def _create_insider_params(self) -> QWidget:
        widget = QWidget()
        layout = QHBoxLayout(widget)

        layout.addWidget(QLabel("检查天数:"))
        self._insider_days_spin = QSpinBox()
        self._insider_days_spin.setRange(1, 365)
        self._insider_days_spin.setValue(30)
        self._insider_days_spin.setSuffix(" 天")
        layout.addWidget(self._insider_days_spin)

        layout.addSpacing(20)
        layout.addWidget(QLabel("返回数量:"))
        self._insider_limit_spin = QSpinBox()
        self._insider_limit_spin.setRange(1, 250)
        self._insider_limit_spin.setValue(50)
        layout.addWidget(self._insider_limit_spin)

        layout.addStretch()
        return widget

    def _create_piotroski_params(self) -> QWidget:
        widget = QWidget()
        layout = QHBoxLayout(widget)

        layout.addWidget(QLabel("最低F-Score:"))
        self._min_fscore_spin = QSpinBox()
        self._min_fscore_spin.setRange(1, 9)
        self._min_fscore_spin.setValue(7)
        layout.addWidget(self._min_fscore_spin)

        layout.addSpacing(20)
        layout.addWidget(QLabel("返回数量:"))
        self._piotroski_limit_spin = QSpinBox()
        self._piotroski_limit_spin.setRange(1, 250)
        self._piotroski_limit_spin.setValue(50)
        layout.addWidget(self._piotroski_limit_spin)

        layout.addStretch()
        return widget

    def _update_param_widgets(self):
        index = self._screener_combo.currentIndex()
        self._rec_widget.setVisible(index == 0)
        self._target_widget.setVisible(index == 1)
        self._insider_widget.setVisible(index == 2)
        self._piotroski_widget.setVisible(index == 3)

    def _init_connections(self):
        self._screener_combo.currentIndexChanged.connect(self._update_param_widgets)
        self._run_btn.clicked.connect(self._run_screener)
        self._add_to_watchlist_btn.clicked.connect(self._add_selected_to_watchlist)

    def _run_screener(self):
        if self._screener_thread and self._screener_thread.isRunning():
            QMessageBox.warning(self, "提示", "选股正在进行中，请稍候")
            return

        index = self._screener_combo.currentIndex()
        screener_type = ''
        params = {}

        if index == 0:
            screener_type = 'recommendation'
            params = {
                'min_buy_ratio': self._min_buy_ratio_spin.value() / 100.0,
                'limit': self._rec_limit_spin.value()
            }
        elif index == 1:
            screener_type = 'price_target'
            params = {
                'min_upside': float(self._min_upside_spin.value()),
                'limit': self._target_limit_spin.value()
            }
        elif index == 2:
            screener_type = 'insider'
            params = {
                'days': self._insider_days_spin.value(),
                'limit': self._insider_limit_spin.value()
            }
        elif index == 3:
            screener_type = 'piotroski'
            params = {
                'min_fscore': self._min_fscore_spin.value(),
                'limit': self._piotroski_limit_spin.value()
            }

        self._run_btn.setEnabled(False)
        self._progress_bar.setRange(0, 0)

        self._screener_thread = ScreenerThread(screener_type, params)
        self._screener_thread.progress_signal.connect(self._on_progress)
        self._screener_thread.finished_signal.connect(self._on_screener_finished)
        self._screener_thread.start()

    def _on_progress(self, current: int, total: int, symbol: str):
        self._progress_label.setText(f"进度: 正在检查 {symbol} ({current}/{total})")
        if total > 0:
            self._progress_bar.setRange(0, total)
            self._progress_bar.setValue(current)

    def _on_screener_finished(self, results: List[Dict[str, Any]]):
        self._run_btn.setEnabled(True)
        self._progress_bar.setRange(0, 100)
        self._progress_bar.setValue(100)
        self._progress_label.setText(f"进度: 完成，找到 {len(results)} 只股票")

        index = self._screener_combo.currentIndex()
        if index == 0:
            self._display_rec_results(results)
        elif index == 1:
            self._display_target_results(results)
        elif index == 2:
            self._display_insider_results(results)
        elif index == 3:
            self._display_piotroski_results(results)

        self._add_to_watchlist_btn.setEnabled(len(results) > 0)

    def _display_rec_results(self, results: List[Dict[str, Any]]):
        self._result_table.clear()
        self._result_table.setColumnCount(11)
        self._result_table.setHorizontalHeaderLabels([
            "股票代码", "名称", "StrongBuy", "Buy", "Hold", "Sell",
            "StrongSell", "买入比例", "当前价格", "涨跌幅", "在自选股"
        ])
        self._result_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)

        self._result_table.setRowCount(len(results))
        for row, item in enumerate(results):
            self._result_table.setItem(row, 0, QTableWidgetItem(item.get('symbol', '')))
            self._result_table.setItem(row, 1, QTableWidgetItem(item.get('name', '')))
            self._result_table.setItem(row, 2, QTableWidgetItem(str(item.get('strongBuy', 0))))
            self._result_table.setItem(row, 3, QTableWidgetItem(str(item.get('buy', 0))))
            self._result_table.setItem(row, 4, QTableWidgetItem(str(item.get('hold', 0))))
            self._result_table.setItem(row, 5, QTableWidgetItem(str(item.get('sell', 0))))
            self._result_table.setItem(row, 6, QTableWidgetItem(str(item.get('strongSell', 0))))
            self._result_table.setItem(row, 7, QTableWidgetItem(f"{item.get('buy_ratio', 0):.1f}%"))

            current_price = item.get('current_price', 0)
            change_pct = item.get('change_percent', 0)

            price_item = QTableWidgetItem(f"{current_price:.2f}" if current_price else "--")
            change_item = QTableWidgetItem(f"{change_pct:+.2f}%" if change_pct else "--")

            if change_pct > 0:
                color = QColor(0, 150, 0)
            elif change_pct < 0:
                color = QColor(200, 0, 0)
            else:
                color = QColor(0, 0, 0)

            price_item.setForeground(color)
            change_item.setForeground(color)

            self._result_table.setItem(row, 8, price_item)
            self._result_table.setItem(row, 9, change_item)

            in_watchlist = self._watchlist_manager.is_in_watchlist(item.get('symbol', ''))
            watchlist_item = QTableWidgetItem("是" if in_watchlist else "否")
            if in_watchlist:
                watchlist_item.setForeground(QColor(0, 150, 0))
            self._result_table.setItem(row, 10, watchlist_item)

    def _display_target_results(self, results: List[Dict[str, Any]]):
        self._result_table.clear()
        self._result_table.setColumnCount(9)
        self._result_table.setHorizontalHeaderLabels([
            "股票代码", "名称", "当前价格", "目标均价", "上涨空间",
            "目标最高", "目标最低", "涨跌幅", "在自选股"
        ])
        self._result_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)

        self._result_table.setRowCount(len(results))
        for row, item in enumerate(results):
            self._result_table.setItem(row, 0, QTableWidgetItem(item.get('symbol', '')))
            self._result_table.setItem(row, 1, QTableWidgetItem(item.get('name', '')))

            current_price = item.get('current_price', 0)
            change_pct = item.get('change_percent', 0)

            price_item = QTableWidgetItem(f"{current_price:.2f}" if current_price else "--")
            change_item = QTableWidgetItem(f"{change_pct:+.2f}%" if change_pct else "--")

            if change_pct > 0:
                color = QColor(0, 150, 0)
            elif change_pct < 0:
                color = QColor(200, 0, 0)
            else:
                color = QColor(0, 0, 0)

            price_item.setForeground(color)
            change_item.setForeground(color)

            self._result_table.setItem(row, 2, price_item)
            self._result_table.setItem(row, 3, QTableWidgetItem(f"{item.get('target_mean', 0):.2f}"))

            upside = item.get('upside_potential', 0)
            upside_item = QTableWidgetItem(f"{upside:+.2f}%")
            upside_item.setForeground(QColor(0, 150, 0))
            self._result_table.setItem(row, 4, upside_item)

            self._result_table.setItem(row, 5, QTableWidgetItem(f"{item.get('target_high', 0):.2f}"))
            self._result_table.setItem(row, 6, QTableWidgetItem(f"{item.get('target_low', 0):.2f}"))
            self._result_table.setItem(row, 7, change_item)

            in_watchlist = self._watchlist_manager.is_in_watchlist(item.get('symbol', ''))
            watchlist_item = QTableWidgetItem("是" if in_watchlist else "否")
            if in_watchlist:
                watchlist_item.setForeground(QColor(0, 150, 0))
            self._result_table.setItem(row, 8, watchlist_item)

    def _display_insider_results(self, results: List[Dict[str, Any]]):
        self._result_table.clear()
        self._result_table.setColumnCount(10)
        self._result_table.setHorizontalHeaderLabels([
            "股票代码", "名称", "当前价格", "涨跌幅", "交易次数",
            "总股数", "总金额", "最新日期", "内部人", "在自选股"
        ])
        self._result_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)

        self._result_table.setRowCount(len(results))
        for row, item in enumerate(results):
            self._result_table.setItem(row, 0, QTableWidgetItem(item.get('symbol', '')))
            self._result_table.setItem(row, 1, QTableWidgetItem(item.get('name', '')))

            current_price = item.get('current_price', 0)
            change_pct = item.get('change_percent', 0)

            price_item = QTableWidgetItem(f"{current_price:.2f}" if current_price else "--")
            change_item = QTableWidgetItem(f"{change_pct:+.2f}%" if change_pct else "--")

            if change_pct > 0:
                color = QColor(0, 150, 0)
            elif change_pct < 0:
                color = QColor(200, 0, 0)
            else:
                color = QColor(0, 0, 0)

            price_item.setForeground(color)
            change_item.setForeground(color)

            self._result_table.setItem(row, 2, price_item)
            self._result_table.setItem(row, 3, change_item)

            self._result_table.setItem(row, 4, QTableWidgetItem(str(item.get('transaction_count', 0))))
            self._result_table.setItem(row, 5, QTableWidgetItem(self._format_number(item.get('total_shares', 0))))
            self._result_table.setItem(row, 6, QTableWidgetItem(self._format_market_cap(item.get('total_value', 0))))
            self._result_table.setItem(row, 7, QTableWidgetItem(item.get('latest_date', '')))
            self._result_table.setItem(row, 8, QTableWidgetItem(item.get('latest_insider', '')))

            in_watchlist = self._watchlist_manager.is_in_watchlist(item.get('symbol', ''))
            watchlist_item = QTableWidgetItem("是" if in_watchlist else "否")
            if in_watchlist:
                watchlist_item.setForeground(QColor(0, 150, 0))
            self._result_table.setItem(row, 9, watchlist_item)

    def _display_piotroski_results(self, results: List[Dict[str, Any]]):
        self._result_table.clear()
        self._result_table.setColumnCount(12)
        self._result_table.setHorizontalHeaderLabels([
            "股票代码", "名称", "F-Score总分", "ROA", "经营现金流",
            "杠杆变化", "流动比率变化", "毛利率变化", "资产周转率变化",
            "当前价格", "涨跌幅", "在自选股"
        ])
        self._result_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)

        self._result_table.setRowCount(len(results))
        for row, item in enumerate(results):
            self._result_table.setItem(row, 0, QTableWidgetItem(item.get('symbol', '')))
            self._result_table.setItem(row, 1, QTableWidgetItem(item.get('name', '')))

            fscore_total = item.get('fscore_total', 0)
            fscore_item = QTableWidgetItem(str(fscore_total))
            if fscore_total >= 8:
                fscore_item.setForeground(QColor(0, 150, 0))
            elif fscore_total >= 6:
                fscore_item.setForeground(QColor(0, 100, 200))
            else:
                fscore_item.setForeground(QColor(200, 0, 0))
            self._result_table.setItem(row, 2, fscore_item)

            current_roa = item.get('current_roa', None)
            if current_roa is not None:
                roa_item = QTableWidgetItem(f"{current_roa*100:.2f}%")
                if current_roa > 0:
                    roa_item.setForeground(QColor(0, 150, 0))
                else:
                    roa_item.setForeground(QColor(200, 0, 0))
                self._result_table.setItem(row, 3, roa_item)
            else:
                self._result_table.setItem(row, 3, QTableWidgetItem("--"))

            current_cfo = item.get('current_cfo', None)
            if current_cfo is not None:
                self._result_table.setItem(row, 4, QTableWidgetItem(self._format_market_cap(current_cfo)))
            else:
                self._result_table.setItem(row, 4, QTableWidgetItem("--"))

            leverage_change = item.get('leverage_change', '未知')
            leverage_item = QTableWidgetItem(leverage_change)
            if leverage_change == '下降':
                leverage_item.setForeground(QColor(0, 150, 0))
            elif leverage_change == '上升':
                leverage_item.setForeground(QColor(200, 0, 0))
            self._result_table.setItem(row, 5, leverage_item)

            current_ratio_change = item.get('current_ratio_change', '未知')
            cr_item = QTableWidgetItem(current_ratio_change)
            if current_ratio_change == '上升':
                cr_item.setForeground(QColor(0, 150, 0))
            elif current_ratio_change == '下降':
                cr_item.setForeground(QColor(200, 0, 0))
            self._result_table.setItem(row, 6, cr_item)

            gross_margin_change = item.get('gross_margin_change', '未知')
            gm_item = QTableWidgetItem(gross_margin_change)
            if gross_margin_change == '上升':
                gm_item.setForeground(QColor(0, 150, 0))
            elif gross_margin_change == '下降':
                gm_item.setForeground(QColor(200, 0, 0))
            self._result_table.setItem(row, 7, gm_item)

            asset_turnover_change = item.get('asset_turnover_change', '未知')
            at_item = QTableWidgetItem(asset_turnover_change)
            if asset_turnover_change == '上升':
                at_item.setForeground(QColor(0, 150, 0))
            elif asset_turnover_change == '下降':
                at_item.setForeground(QColor(200, 0, 0))
            self._result_table.setItem(row, 8, at_item)

            current_price = item.get('current_price', 0)
            change_pct = item.get('change_percent', 0)

            price_item = QTableWidgetItem(f"{current_price:.2f}" if current_price else "--")
            change_item = QTableWidgetItem(f"{change_pct:+.2f}%" if change_pct else "--")

            if change_pct > 0:
                color = QColor(0, 150, 0)
            elif change_pct < 0:
                color = QColor(200, 0, 0)
            else:
                color = QColor(0, 0, 0)

            price_item.setForeground(color)
            change_item.setForeground(color)

            self._result_table.setItem(row, 9, price_item)
            self._result_table.setItem(row, 10, change_item)

            in_watchlist = self._watchlist_manager.is_in_watchlist(item.get('symbol', ''))
            watchlist_item = QTableWidgetItem("是" if in_watchlist else "否")
            if in_watchlist:
                watchlist_item.setForeground(QColor(0, 150, 0))
            self._result_table.setItem(row, 11, watchlist_item)

    def _format_number(self, num: float) -> str:
        if num >= 1_000_000:
            return f"{num/1_000_000:.2f}M"
        elif num >= 1_000:
            return f"{num/1_000:.2f}K"
        return f"{num:.0f}" if num else "--"

    def _format_market_cap(self, cap: float) -> str:
        if cap >= 1_000_000_000_000:
            return f"{cap/1_000_000_000_000:.2f}T"
        elif cap >= 1_000_000_000:
            return f"{cap/1_000_000_000:.2f}B"
        elif cap >= 1_000_000:
            return f"{cap/1_000_000:.2f}M"
        return f"{cap:.0f}" if cap else "--"

    def _add_selected_to_watchlist(self):
        selected = self._result_table.selectedItems()
        if not selected:
            QMessageBox.warning(self, "提示", "请先选择要添加的股票")
            return

        rows = set(item.row() for item in selected)
        added = []
        already_in = []

        for row in rows:
            symbol_item = self._result_table.item(row, 0)
            if symbol_item:
                symbol = symbol_item.text()
                if self._watchlist_manager.is_in_watchlist(symbol):
                    already_in.append(symbol)
                else:
                    if self._watchlist_manager.add_stock(symbol):
                        added.append(symbol)

        messages = []
        if added:
            messages.append(f"已添加: {', '.join(added)}")
        if already_in:
            messages.append(f"已在自选股中: {', '.join(already_in)}")

        if messages:
            QMessageBox.information(self, "结果", "\n".join(messages))

    def refresh(self):
        pass


class SettingsTab(QWidget):
    settings_changed = pyqtSignal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self._config = ConfigManager()
        self._init_ui()
        self._load_settings()

    def _init_ui(self):
        layout = QVBoxLayout(self)
        layout.setSpacing(20)

        general_group = QGroupBox("常规设置")
        general_layout = QVBoxLayout(general_group)

        refresh_layout = QHBoxLayout()
        refresh_layout.addWidget(QLabel("数据刷新间隔 (秒):"))
        self._refresh_interval_spin = QSpinBox()
        self._refresh_interval_spin.setRange(10, 3600)
        self._refresh_interval_spin.setValue(60)
        self._refresh_interval_spin.setSuffix(" 秒")
        refresh_layout.addWidget(self._refresh_interval_spin)
        refresh_layout.addStretch()
        general_layout.addLayout(refresh_layout)

        auto_layout = QHBoxLayout()
        self._auto_refresh_check = QCheckBox("启用自动刷新")
        auto_layout.addWidget(self._auto_refresh_check)
        auto_layout.addStretch()
        general_layout.addLayout(auto_layout)

        layout.addWidget(general_group)

        news_group = QGroupBox("新闻设置")
        news_layout = QVBoxLayout(news_group)

        news_count_layout = QHBoxLayout()
        news_count_layout.addWidget(QLabel("默认获取新闻数量:"))
        self._news_count_spin = QSpinBox()
        self._news_count_spin.setRange(1, 50)
        self._news_count_spin.setValue(10)
        news_count_layout.addWidget(self._news_count_spin)
        news_count_layout.addStretch()
        news_layout.addLayout(news_count_layout)

        layout.addWidget(news_group)

        screener_group = QGroupBox("选股设置")
        screener_layout = QVBoxLayout(screener_group)

        screener_limit_layout = QHBoxLayout()
        screener_limit_layout.addWidget(QLabel("默认选股返回数量:"))
        self._screener_limit_spin = QSpinBox()
        self._screener_limit_spin.setRange(1, 250)
        self._screener_limit_spin.setValue(50)
        screener_limit_layout.addWidget(self._screener_limit_spin)
        screener_limit_layout.addStretch()
        screener_layout.addLayout(screener_limit_layout)

        layout.addWidget(screener_group)

        btn_layout = QHBoxLayout()
        self._save_btn = QPushButton("保存设置")
        self._save_btn.setMinimumHeight(40)
        btn_layout.addWidget(self._save_btn)

        self._reset_btn = QPushButton("恢复默认")
        self._reset_btn.setMinimumHeight(40)
        btn_layout.addWidget(self._reset_btn)

        btn_layout.addStretch()
        layout.addLayout(btn_layout)

        layout.addStretch()

        self._save_btn.clicked.connect(self._save_settings)
        self._reset_btn.clicked.connect(self._reset_settings)

    def _load_settings(self):
        self._refresh_interval_spin.setValue(self._config.refresh_interval)
        self._auto_refresh_check.setChecked(self._config.auto_refresh)
        self._news_count_spin.setValue(self._config.news_count)
        self._screener_limit_spin.setValue(self._config.screener_limit)

    def _save_settings(self):
        self._config.refresh_interval = self._refresh_interval_spin.value()
        self._config.auto_refresh = self._auto_refresh_check.isChecked()
        self._config.news_count = self._news_count_spin.value()
        self._config.screener_limit = self._screener_limit_spin.value()

        self.settings_changed.emit()
        QMessageBox.information(self, "成功", "设置已保存")

    def _reset_settings(self):
        reply = QMessageBox.question(
            self, "确认",
            "确定要恢复默认设置吗？",
            QMessageBox.Yes | QMessageBox.No
        )
        if reply == QMessageBox.Yes:
            self._config.refresh_interval = 60
            self._config.auto_refresh = True
            self._config.news_count = 10
            self._config.screener_limit = 25
            self._load_settings()
            self.settings_changed.emit()
            QMessageBox.information(self, "成功", "已恢复默认设置")

    def refresh(self):
        self._load_settings()


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self._config = ConfigManager()
        self._watchlist_manager = WatchlistManager()
        self._init_ui()
        self._init_timer()
        self._init_menu_bar()

    def _init_ui(self):
        self.setWindowTitle("股票监控应用")
        self.setMinimumSize(1200, 800)

        central_widget = QWidget()
        self.setCentralWidget(central_widget)

        layout = QVBoxLayout(central_widget)
        layout.setContentsMargins(5, 5, 5, 5)

        self._tab_widget = QTabWidget()

        self._watchlist_tab = WatchlistTab()
        self._news_tab = NewsTab()
        self._screener_tab = ScreenerTab()
        self._settings_tab = SettingsTab()

        self._tab_widget.addTab(self._watchlist_tab, "自选股")
        self._tab_widget.addTab(self._news_tab, "资讯")
        self._tab_widget.addTab(self._screener_tab, "选股工具")
        self._tab_widget.addTab(self._settings_tab, "设置")

        self._tab_widget.currentChanged.connect(self._on_tab_changed)

        layout.addWidget(self._tab_widget)

        self._status_bar = QStatusBar()
        self.setStatusBar(self._status_bar)
        self._status_bar.showMessage("就绪")

    def _init_timer(self):
        self._refresh_timer = QTimer(self)
        self._refresh_timer.timeout.connect(self._auto_refresh)
        self._update_timer_interval()

        if self._config.auto_refresh:
            self._refresh_timer.start()

        self._settings_tab.settings_changed.connect(self._on_settings_changed)

    def _init_menu_bar(self):
        menubar = self.menuBar()

        file_menu = menubar.addMenu("文件(&F)")

        refresh_action = QAction("刷新数据(&R)", self)
        refresh_action.setShortcut("F5")
        refresh_action.triggered.connect(self._refresh_current_tab)
        file_menu.addAction(refresh_action)

        file_menu.addSeparator()

        exit_action = QAction("退出(&X)", self)
        exit_action.setShortcut("Ctrl+Q")
        exit_action.triggered.connect(self.close)
        file_menu.addAction(exit_action)

        view_menu = menubar.addMenu("视图(&V)")

        watchlist_action = QAction("自选股(&W)", self)
        watchlist_action.triggered.connect(lambda: self._tab_widget.setCurrentIndex(0))
        view_menu.addAction(watchlist_action)

        news_action = QAction("资讯(&N)", self)
        news_action.triggered.connect(lambda: self._tab_widget.setCurrentIndex(1))
        view_menu.addAction(news_action)

        screener_action = QAction("选股工具(&S)", self)
        screener_action.triggered.connect(lambda: self._tab_widget.setCurrentIndex(2))
        view_menu.addAction(screener_action)

        settings_action = QAction("设置(&T)", self)
        settings_action.triggered.connect(lambda: self._tab_widget.setCurrentIndex(3))
        view_menu.addAction(settings_action)

        help_menu = menubar.addMenu("帮助(&H)")

        about_action = QAction("关于(&A)", self)
        about_action.triggered.connect(self._show_about)
        help_menu.addAction(about_action)

    def _update_timer_interval(self):
        self._refresh_timer.setInterval(self._config.refresh_interval * 1000)

    def _auto_refresh(self):
        if self._config.auto_refresh:
            current_index = self._tab_widget.currentIndex()
            if current_index == 0:
                self._watchlist_tab.refresh()
            self._status_bar.showMessage(f"自动刷新于 {datetime.now().strftime('%H:%M:%S')}")

    def _on_tab_changed(self, index: int):
        if index == 0:
            self._status_bar.showMessage("自选股页面")
        elif index == 1:
            self._status_bar.showMessage("资讯页面")
        elif index == 2:
            self._status_bar.showMessage("选股工具页面")
        elif index == 3:
            self._status_bar.showMessage("设置页面")

    def _refresh_current_tab(self):
        current_index = self._tab_widget.currentIndex()
        if current_index == 0:
            self._watchlist_tab.refresh()
        elif current_index == 1:
            self._news_tab.refresh()
        elif current_index == 2:
            self._screener_tab.refresh()
        elif current_index == 3:
            self._settings_tab.refresh()

        self._status_bar.showMessage(f"手动刷新于 {datetime.now().strftime('%H:%M:%S')}")

    def _on_settings_changed(self):
        self._update_timer_interval()
        if self._config.auto_refresh and not self._refresh_timer.isActive():
            self._refresh_timer.start()
        elif not self._config.auto_refresh and self._refresh_timer.isActive():
            self._refresh_timer.stop()

    def _show_about(self):
        QMessageBox.about(
            self,
            "关于",
            "股票监控应用 v1.0.0\n\n"
            "基于 yfinance 和 PyQt5 开发\n\n"
            "功能:\n"
            "- 自选股管理\n"
            "- 实时行情监控\n"
            "- 新闻资讯获取\n"
            "- 智能选股工具"
        )

    def closeEvent(self, event):
        self._watchlist_manager.stop_auto_refresh()
        if self._refresh_timer.isActive():
            self._refresh_timer.stop()
        event.accept()
