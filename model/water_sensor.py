#!/usr/bin/python
# -*- coding:UTF-8 -*-


class WaterSensor:
    def __init__(self, id, timestamp, value):
        self.id = id
        self.timestamp = timestamp
        self.value = value

    def to_dict(self):
        return {
            'id': self.id,
            'timestamp': self.timestamp,
            'value': self.value
        }

    def __repr__(self):
        return f"WaterSensor(id={self.id}, timestamp={self.timestamp}, value={self.value})"
