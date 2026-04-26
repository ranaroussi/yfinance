from PyQt5.QtWidgets import QWidget, QLabel, QVBoxLayout, QHBoxLayout
from PyQt5.QtCore import Qt, QRectF
from PyQt5.QtGui import QPainter, QColor, QFont, QPen, QBrush, QPolygonF, QLinearGradient
from typing import List, Dict, Any, Optional
import math


class RadarChartWidget(QWidget):
    DIMENSIONS = [
        ('PE评分', 0.15),
        ('PB评分', 0.15),
        ('PEG评分', 0.10),
        ('安全边际', 0.15),
        ('预期增长', 0.15),
        ('盈利能力', 0.10),
        ('股息收益', 0.05),
        ('财务健康', 0.05),
        ('现金流', 0.05),
        ('综合质量', 0.05),
    ]

    def __init__(self, parent=None):
        super().__init__(parent)
        self._scores: List[float] = [0.0] * 10
        self._total_score: float = 0.0
        self._status: str = ''
        self._status_color: QColor = QColor(100, 100, 100)
        self.setMinimumSize(400, 400)

    def set_scores(self, scores: Dict[str, Any], total_score: float, status: str):
        self._scores = [
            scores.get('pe_score', 0),
            scores.get('pb_score', 0),
            scores.get('peg_score', 0),
            scores.get('margin_score', 0),
            scores.get('growth_score', 0),
            scores.get('profit_score', 0),
            scores.get('dividend_score', 0),
            scores.get('financial_health_score', 0),
            scores.get('fcf_score', 0),
            scores.get('quality_score', 0),
        ]
        self._total_score = total_score
        self._status = status

        if total_score >= 80:
            self._status_color = QColor(34, 197, 94)
        elif total_score >= 65:
            self._status_color = QColor(59, 130, 246)
        elif total_score >= 50:
            self._status_color = QColor(168, 162, 158)
        elif total_score >= 35:
            self._status_color = QColor(251, 146, 60)
        else:
            self._status_color = QColor(239, 68, 68)

        self.update()

    def clear(self):
        self._scores = [0.0] * 10
        self._total_score = 0.0
        self._status = ''
        self.update()

    def paintEvent(self, event):
        painter = QPainter(self)
        painter.setRenderHint(QPainter.Antialiasing)

        width = self.width()
        height = self.height()
        center_x = width // 2
        center_y = height // 2
        max_radius = min(center_x, center_y) - 60

        self._draw_grid(painter, center_x, center_y, max_radius)
        self._draw_data(painter, center_x, center_y, max_radius)
        self._draw_labels(painter, center_x, center_y, max_radius)
        self._draw_legend(painter, width, height)

    def _draw_grid(self, painter: QPainter, cx: int, cy: int, radius: int):
        painter.setPen(QPen(QColor(200, 200, 200), 1, Qt.SolidLine))
        painter.setBrush(Qt.NoBrush)

        num_levels = 5
        for i in range(num_levels + 1):
            r = int(radius * i / num_levels)
            painter.drawEllipse(cx - r, cy - r, r * 2, r * 2)

        num_axes = len(self.DIMENSIONS)
        for i in range(num_axes):
            angle = (i * 2 * math.pi / num_axes) - math.pi / 2
            x = cx + int(radius * math.cos(angle))
            y = cy + int(radius * math.sin(angle))
            painter.drawLine(cx, cy, x, y)

    def _draw_data(self, painter: QPainter, cx: int, cy: int, radius: int):
        num_axes = len(self.DIMENSIONS)

        points = []
        for i in range(num_axes):
            score = self._scores[i]
            r = int(radius * score / 100.0)
            angle = (i * 2 * math.pi / num_axes) - math.pi / 2
            x = cx + int(r * math.cos(angle))
            y = cy + int(r * math.sin(angle))
            points.append((x, y))

        if any(self._scores):
            polygon = QPolygonF()
            for x, y in points:
                polygon.append((x, y))

            fill_color = QColor(self._status_color.red(), self._status_color.green(), 
                               self._status_color.blue(), 80)
            painter.setBrush(QBrush(fill_color))
            painter.setPen(QPen(self._status_color, 2, Qt.SolidLine))
            painter.drawPolygon(polygon)

            for x, y in points:
                painter.setBrush(QBrush(self._status_color))
                painter.drawEllipse(x - 4, y - 4, 8, 8)

    def _draw_labels(self, painter: QPainter, cx: int, cy: int, radius: int):
        num_axes = len(self.DIMENSIONS)
        label_radius = radius + 25

        painter.setFont(QFont('Microsoft YaHei', 9))
        painter.setPen(QPen(QColor(60, 60, 60), 1))

        for i in range(num_axes):
            name, _ = self.DIMENSIONS[i]
            angle = (i * 2 * math.pi / num_axes) - math.pi / 2
            x = cx + int(label_radius * math.cos(angle))
            y = cy + int(label_radius * math.sin(angle))

            score = self._scores[i]

            text = f'{name}\n{int(score)}分'

            if abs(math.cos(angle)) < 0.1:
                align = Qt.AlignHCenter
            elif math.cos(angle) > 0:
                align = Qt.AlignLeft
            else:
                align = Qt.AlignRight

            if abs(math.sin(angle)) < 0.1:
                align |= Qt.AlignVCenter
            elif math.sin(angle) > 0:
                align |= Qt.AlignTop
            else:
                align |= Qt.AlignBottom

            if align & Qt.AlignLeft:
                draw_x = x
            elif align & Qt.AlignRight:
                draw_x = x - 60
            else:
                draw_x = x - 30

            if align & Qt.AlignTop:
                draw_y = y
            elif align & Qt.AlignBottom:
                draw_y = y - 30
            else:
                draw_y = y - 15

            painter.drawText(draw_x, draw_y, 60, 30, 
                           Qt.AlignCenter | Qt.TextWordWrap, text)

    def _draw_legend(self, painter: QPainter, width: int, height: int):
        if self._total_score <= 0:
            return

        painter.setFont(QFont('Microsoft YaHei', 12, QFont.Bold))
        painter.setPen(QPen(self._status_color, 1))

        legend_text = f'综合评分: {int(self._total_score)}分'
        if self._status:
            legend_text += f'  ({self._status})'

        painter.drawText(0, height - 30, width, 25, Qt.AlignCenter, legend_text)


class DimensionScoreBar(QWidget):
    def __init__(self, name: str, weight: float, parent=None):
        super().__init__(parent)
        self._name = name
        self._weight = weight
        self._score = 0.0
        self._max_score = 100.0
        self.setMinimumHeight(35)

    def set_score(self, score: float):
        self._score = score
        self.update()

    def get_score(self) -> float:
        return self._score

    def paintEvent(self, event):
        painter = QPainter(self)
        painter.setRenderHint(QPainter.Antialiasing)

        width = self.width()
        height = self.height()
        padding = 10

        label_width = 100
        value_width = 60
        bar_start_x = padding + label_width
        bar_width = width - bar_start_x - value_width - padding
        bar_height = height - 2 * padding

        score_ratio = self._score / self._max_score if self._max_score > 0 else 0

        painter.setFont(QFont('Microsoft YaHei', 9))

        painter.setPen(QPen(QColor(60, 60, 60), 1))
        painter.drawText(padding, 0, label_width, height, 
                        Qt.AlignVCenter | Qt.AlignLeft, 
                        f'{self._name} ({int(self._weight * 100)}%)')

        painter.setPen(Qt.NoPen)
        painter.setBrush(QBrush(QColor(230, 230, 230)))
        painter.drawRoundedRect(QRectF(bar_start_x, padding, bar_width, bar_height), 5, 5)

        fill_width = bar_width * score_ratio
        if fill_width > 0:
            if score_ratio >= 0.7:
                color = QColor(34, 197, 94)
            elif score_ratio >= 0.5:
                color = QColor(59, 130, 246)
            elif score_ratio >= 0.3:
                color = QColor(251, 146, 60)
            else:
                color = QColor(239, 68, 68)

            painter.setBrush(QBrush(color))
            painter.drawRoundedRect(QRectF(bar_start_x, padding, fill_width, bar_height), 5, 5)

        painter.setPen(QPen(QColor(60, 60, 60), 1))
        value_x = bar_start_x + bar_width + 5
        painter.drawText(value_x, 0, value_width, height, 
                        Qt.AlignVCenter | Qt.AlignRight, 
                        f'{int(self._score)}分')


class TotalScoreDisplay(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._score = 0.0
        self._status = ''
        self._status_color = QColor(100, 100, 100)
        self.setMinimumSize(150, 150)

    def set_score(self, score: float, status: str):
        self._score = score
        self._status = status

        if score >= 80:
            self._status_color = QColor(34, 197, 94)
        elif score >= 65:
            self._status_color = QColor(59, 130, 246)
        elif score >= 50:
            self._status_color = QColor(168, 162, 158)
        elif score >= 35:
            self._status_color = QColor(251, 146, 60)
        else:
            self._status_color = QColor(239, 68, 68)

        self.update()

    def clear(self):
        self._score = 0.0
        self._status = ''
        self.update()

    def paintEvent(self, event):
        painter = QPainter(self)
        painter.setRenderHint(QPainter.Antialiasing)

        width = self.width()
        height = self.height()
        center_x = width // 2
        center_y = height // 2
        radius = min(center_x, center_y) - 15

        score_ratio = self._score / 100.0 if self._score > 0 else 0

        painter.setPen(QPen(QColor(220, 220, 220), 12, Qt.SolidLine, Qt.RoundCap))
        start_angle = 90 * 16
        span_angle = 360 * 16
        painter.drawArc(center_x - radius, center_y - radius, 
                       radius * 2, radius * 2, start_angle, span_angle)

        if score_ratio > 0:
            gradient = QLinearGradient(0, center_y - radius, 0, center_y + radius)
            gradient.setColorAt(0, self._status_color.lighter(120))
            gradient.setColorAt(1, self._status_color.darker(120))

            painter.setPen(QPen(gradient, 12, Qt.SolidLine, Qt.RoundCap))
            span_angle = int(360 * 16 * score_ratio)
            painter.drawArc(center_x - radius, center_y - radius, 
                           radius * 2, radius * 2, start_angle, -span_angle)

        if self._score > 0:
            painter.setFont(QFont('Microsoft YaHei', 28, QFont.Bold))
            painter.setPen(QPen(self._status_color, 1))
            score_text = f'{int(self._score)}'
            painter.drawText(center_x - 40, center_y - 30, 80, 60, 
                            Qt.AlignCenter, score_text)

            painter.setFont(QFont('Microsoft YaHei', 11))
            painter.setPen(QPen(QColor(100, 100, 100), 1))
            painter.drawText(center_x - 50, center_y + 20, 100, 25, 
                            Qt.AlignCenter, self._status)
        else:
            painter.setFont(QFont('Microsoft YaHei', 12))
            painter.setPen(QPen(QColor(150, 150, 150), 1))
            painter.drawText(center_x - 50, center_y - 15, 100, 30, 
                            Qt.AlignCenter, '等待分析')
