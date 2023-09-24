#!/usr/bin/python
# -*- coding:UTF-8 -*-


class StockInfo(object):
    """
    股票模型
    """
    def __init__(self, stock_code, stock_name, vol):
        self.stock_code = stock_code
        self.stock_name = stock_name
        self.vol = vol

    def to_dict(self):
        return {
            'stock_code': self.stock_code,
            'stock_name': self.stock_name,
            'vol': self.vol
        }

    def __repr__(self):
        return f"StockInfo(stock_code={self.stock_code}, stock_name={self.stock_name}, vol={self.vol})"
