#!/usr/bin/python
# -*- coding:UTF-8 -*-


class WaterSensor:
    def __init__(self, id, vc, ts):
        self.id = id
        self.vc = vc
        self.ts = ts

    def to_dict(self):
        return {
            'id': self.id,
            'vc': self.vc,
            'ts': self.ts
        }

    def __repr__(self):
        return f"WaterSensor(id={self.id}, vc={self.vc}, ts={self.ts})"
