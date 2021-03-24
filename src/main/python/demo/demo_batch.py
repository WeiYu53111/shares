import logging
import os
import shutil
import sys
import tempfile

from pyflink.dataset import ExecutionEnvironment
from pyflink.table import BatchTableEnvironment, TableConfig
from pyflink.table import expressions as expr


def word_count():
    content = "line Licensed to the Apache Software Foundation ASF under one " \
              "line or more contributor license agreements See the NOTICE file " \
              "line distributed with this work for additional information " \
              "line regarding copyright ownership The ASF licenses this file " \
              "to you under the Apache License Version the " \
              "License you may not use this file except in compliance " \
              "with the License"

    exec_env = ExecutionEnvironment.get_execution_environment()
    exec_env.set_parallelism(1)
    t_config = TableConfig()
    t_env = BatchTableEnvironment.create(exec_env, t_config)

    # register Results table in table environment
    tmp_dir = tempfile.gettempdir()
    result_path = tmp_dir + '/result'
    if os.path.exists(result_path):
        try:
            if os.path.isfile(result_path):
                os.remove(result_path)
            else:
                shutil.rmtree(result_path)
        except OSError as e:
            logging.error("Error removing directory: %s - %s.", e.filename, e.strerror)

    logging.info("Results directory: %s", result_path)

    sink_ddl = """
        create table Results(
            word VARCHAR,
            `count` BIGINT
        ) with (
            'connector.type' = 'filesystem',
            'format.type' = 'csv',
            'connector.path' = '{}'
        )
        """.format(result_path)
    t_env.execute_sql(sink_ddl)

    elements = [(word, 1) for word in content.split(" ")]
    table = t_env.from_elements(elements, ["word", "count"])
    table.group_by(table.word) \
        .select(table.word, expr.lit(1).count.alias('count'))\
        .execute_insert("Results").wait()



    #logging.info("job done", result_path)
    # t_env.execute_sql("123")
    # t_env.execute_sql("word_count")


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
    word_count()