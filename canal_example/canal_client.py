#!/usr/bin/python
# -*- coding:UTF-8 -*-
import time
import json

from canal.client import Client
from canal.protocol import EntryProtocol_pb2
from canal.protocol import CanalProtocol_pb2


def data_to_sql(data: dict) -> str:
    db = data['db']
    table = data['db']
    # insert
    if data['event_type'] == 1:
        dic_data = data['data']['after']
        insert_value = ""
        for key in dic_data.keys():
            insert_value = insert_value + f"'{dic_data[key]}'" + ','
            insert_value = insert_value[:-1]
        sql = f"insert into {db}.{table} values ({insert_value});"
        return sql
    # update
    elif data['event_type'] == 2:
        before_data = data['data']['before']
        after_data = data['data']['after']
        update_value = ""
        update_condition = ""
        for key in before_data.keys():
            update_condition = update_condition + f"'{before_data[key]}' and "
            update_condition = update_condition[:-5]
        for key in after_data.keys():
            update_value = update_value + key + f"='{after_data[key]}',"
            update_value = update_value[:-1]
        sql = f"update {db}.{table} set {update_value} where {update_condition};"
        return sql
        # delete
    else:
        dic_data = data['data']['before']
        delete_condition = ""
        for key in dic_data.keys():
            delete_condition = delete_condition + f"'{dic_data[key]}' and "
            delete_condition = delete_condition[:-5]
        sql = f"delete from {db}.{table} where {delete_condition};"
        return sql


def get_canal_data_example(conn):
    """
    Canal 实时监听 MySQL 数据变化
    """
    while True:
        message = conn.get(100)
        # 获取entries集合
        entries = message['entries']
        # 判断集合是否为空，如果为空，则等待一会继续拉取
        if len(entries) <= 0:
            # print('当前抓取没有数据，休息一会......')
            time.sleep(1)
        else:
            # 遍历 entries，单条解析
            for entry in entries:
                entry_type = entry.entryType
                if entry_type in [EntryProtocol_pb2.EntryType.TRANSACTIONBEGIN,
                                  EntryProtocol_pb2.EntryType.TRANSACTIONEND]:
                    continue
                row_change = EntryProtocol_pb2.RowChange()
                row_change.MergeFromString(entry.storeValue)
                header = entry.header
                # 数据库名
                database = header.schemaName
                # 表名
                table = header.tableName
                event_type = header.eventType
                # row是binlog解析出来的行变化记录，一般有三种格式，对应增删改
                items = []
                for row in row_change.rowDatas:
                    format_data = dict()
                    # 根据增删改的其中一种情况进行数据处理
                    if event_type == EntryProtocol_pb2.EventType.DELETE:
                        format_data['before'] = dict()
                        for column in row.beforeColumns:
                            format_data = {
                                column.name: column.value
                            }
                            # 此处注释为原demo，有误，下面是正确写法
                            # format_data['before'][column.name] = column.value
                    elif event_type == EntryProtocol_pb2.EventType.INSERT:
                        format_data['after'] = dict()
                        for column in row.afterColumns:
                            # format_data = {
                            #     column.name: column.value
                            # }
                            format_data['after'][column.name] = column.value
                    else:
                        # format_data['before'] = format_data['after'] = dict()
                        format_data['before'] = dict()
                        format_data['after'] = dict()
                        for column in row.beforeColumns:
                            format_data['before'][column.name] = column.value
                        for column in row.afterColumns:
                            format_data['after'][column.name] = column.value
                    # data即最后获取的数据，包含库名，表明，事务类型，改动数据
                    data = dict(
                        db=database,
                        table=table,
                        event_type=event_type,
                        data=format_data,
                    )
                    items.append(data)
                    # print(data)
                print(json.dumps(items, indent=4, ensure_ascii=False))
                print(len(items))
                print('****************************')
            time.sleep(1)


if __name__ == '__main__':
    # TODO 建立与canal服务端的连接
    # 实例化Client对象
    client = Client()
    # canal服务端部署的主机IP与端口
    client.connect(host='node01', port=11111)
    # 数据库账户、密码
    client.check_valid(username=b'root', password=b'Oscar&0503')
    # destination是canal服务端的服务名称， filter即获取数据的过滤规则，采用正则表达式
    client.subscribe(client_id=b'1001', destination=b'example', filter=b'.*\\..*')

    # get_canal_data(conn=client)

    get_canal_data_example(conn=client)

    client.disconnect()
