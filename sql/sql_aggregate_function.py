#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes
from pyflink.table.expressions import col
from pyflink.table.functions import AggregateFunction
from pyflink.table.udf import udf


class ScoreWeightedAvg(AggregateFunction):
    """
    继承 AggregateFunction
    """

    def get_value(self, accumulator):
        # sum / count
        if accumulator[0] != 0:
            return accumulator[1] / accumulator[0]
        else:
            return None

    def create_accumulator(self) -> list:
        return [0, 0]

    def accumulate(self, accumulator: tuple, *args):
        """
        累加计算的方法，每来一行数据都会调用一次
        :param accumulator: 累加器类型
        :param args: 参数
        :return:
        """
        if args[0] is not None:
            accumulator[0] += 1
            accumulator[1] += args[0]

    def merge(self, accumulator, accumulators):
        for acc in accumulators:
            if acc[1] is not None:
                accumulator[0] += acc[0]
                accumulator[1] += acc[1]

    def retract(self, accumulator, *args):
        if args[0] is not None:
            accumulator[0] -= 1
            accumulator[1] -= args[0]


def aggregate_function_demo():
    env = StreamExecutionEnvironment.get_execution_environment()
    table_env = StreamTableEnvironment.create(env)

    # 姓名，分数，权重
    score_weight_data = [
        ("zs", 80, 3),
        ("zs", 90, 4),
        ("zs", 95, 4),
        ("ls", 75, 4),
        ("ls", 65, 4),
        ("ls", 85, 4)
    ]

    score_weight_ds = env.from_collection(
        collection=score_weight_data, type_info=Types.ROW([Types.STRING(), Types.INT(), Types.INT()]))

    table_env.create_temporary_view(
        "scores",
        score_weight_ds,
        col('name'),
        col('score'),
        col('weight')
    )

    # TODO 2.注册函数
    # table_env.create_temporary_function('WeightedAvg', ScoreWeightedAvg())
    table_env.create_temporary_function('WeightedAvg', ScoreWeightedAvg())

    # TODO 3.调用 自定义函数
    table_env.sql_query('select name,WeightedAvg(score,weight)  from scores group by name;').execute().print()


if __name__ == '__main__':
    aggregate_function_demo()
