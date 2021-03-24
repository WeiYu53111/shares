import logging
import os
import shutil
import sys
import tempfile

from pyflink.dataset import ExecutionEnvironment
from pyflink.table import BatchTableEnvironment, TableConfig
from pyflink.table import expressions as expr



def load(token):

    exec_env = ExecutionEnvironment.get_execution_environment()
    exec_env.set_parallelism(1)
    t_config = TableConfig()
    t_env = BatchTableEnvironment.create(exec_env, t_config)







if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    # todo 从配置文件读取
    token = "c3481fba287edd683b466c3ae1028d2f03f661fb537db48d1bb79e21"
    load(token)