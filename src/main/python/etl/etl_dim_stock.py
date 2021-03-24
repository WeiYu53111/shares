# encoding=utf-8
import logging
import sys
import tushare as ts
from pyflink.table import EnvironmentSettings, BatchTableEnvironment


def load(token):
    # 获取交易日期维度数
    pro = ts.pro_api(token)
    df = pro.query('stock_basic',list_status='L',fields='ts_code,symbol,name,area,industry,market,curr_type,list_date,is_hs')

    # 创建flink程序的入口
    env_settings = EnvironmentSettings.new_instance().in_batch_mode().use_blink_planner().build()
    table_env = BatchTableEnvironment.create(environment_settings=env_settings)

    # 将pandas的dataframe转换成 table,并通过创建视图的方式赋予别称
    table = table_env.from_pandas(df)
    table_env.create_temporary_view("stock_info", table)
    # 声明输出的
    sink_ddl = """
    -- register a MySQL table 'users' in Flink SQL
    create table Results(
        ts_code STRING,
        symbol STRING,
        name  STRING,
        area   STRING,
        industry  STRING,
        market    STRING,
        curr_type STRING,
        list_date  STRING,
        is_hs  STRING
    ) with (
       'connector' = 'jdbc',
       'url' = 'jdbc:mysql://localhost:3306/shares?useUnicode=yes&characterEncoding=UTF-8&useSSL=false',
       'table-name' = 'dim_stock',
       'username' = 'root',
       'password' = '123456'
    )
    """
    table_env.execute_sql(sink_ddl)

    # 使用jdbc方式需要额外添加java的jar
    table_env.get_config().get_configuration().set_string("pipeline.jars",
                                                          "file:///home/wy/shares/mysql-connector-java-5.1.49.jar;file:///home/wy/shares/flink-connector-jdbc_2.12-1.12.2.jar")

    # mini模式运行的时候需要调用wait 等待 程序运行完成
    table_env.execute_sql("insert into Results select * from stock_info").wait()


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    # todo 从配置文件读取
    token = "c3481fba287edd683b466c3ae1028d2f03f661fb537db48d1bb79e21"

    # 时间范围
    load(token)
