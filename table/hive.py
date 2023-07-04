#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyflink.table import TableEnvironment, EnvironmentSettings
from pyflink.table.catalog import HiveCatalog

if __name__ == '__main__':
    settings = EnvironmentSettings.in_batch_mode()
    t_env = TableEnvironment.create(settings)
    # Create a HiveCatalog
    catalog = HiveCatalog("hive_catalog", None, "<path_of_hive_conf>")

    # Register the catalog
    t_env.register_catalog("hive_catalog", catalog)

    # Create a catalog database
    t_env.execute_sql("CREATE DATABASE mydb WITH (...)")

    # Create a catalog table
    t_env.execute_sql("CREATE TABLE mytable (name STRING, age INT) WITH (...)")

    # should return the tables in current catalog and database.
    t_env.list_tables()