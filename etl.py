import argparse
import pymysql
from peer_assess_pro import PeerAssessPro
from sword import Sword
from DatabaseExtract import DatabaseExtract
from etl_table import (
    ACTOR_PARTICIPANTS, ACTORS, ANSWERS, ARTIFACTS, CRITERIA, 
    EVAL_MODES, ITEMS, PARTICIPANTS, TASKS
)
from conf import Conf
import petl as etl

class LoadToDatabase(object):
    def __init__(self, etl_table):
        self.conf = Conf()
        self.etl_table = etl_table
        # TODO: It should be possible to grab this from the schema
        self.UPDATE_ORDER = [
            PARTICIPANTS, ACTORS, ACTOR_PARTICIPANTS, CRITERIA, EVAL_MODES,
            TASKS, ITEMS, ARTIFACTS, ANSWERS,
        ]

    def load_to_warehouse(self, db_info):
        connection = pymysql.connect(
            host=db_info['host'], user=db_info['user'],
            password=db_info['passwd'], db=db_info['db'],
        )
        connection.cursor().execute('SET SQL_MODE=ANSI_QUOTES')
        for table in self.UPDATE_ORDER:
            data = self.etl_table.TABLES[table]()
            print(f'Loading {table}...\n{data}')
            columns = ','.join(etl.header(data))
            values = ','.join(['%s']*len(etl.header(data)))
            duplicate_updates = ','.join(
                [f'{column} = VALUES({column})' for column in etl.header(data)]
            )
            query = f"INSERT {table} ({columns}) VALUES ({values}) ON DUPLICATE KEY UPDATE {duplicate_updates};"
            print(query)
            connection.cursor().executemany(query, etl.records(data))
        connection.close()

class LoadToStaging(LoadToDatabase):
    def __init__(self, etl_table):
        super(LoadToStaging, self).__init__(etl_table)

    def load_to_warehouse(self):
        super(LoadToStaging, self).load_to_warehouse(self.conf.get_staging_db_info())

class LoadToDatawarehouse(LoadToDatabase):
    def __init__(self, etl_table):
        super(LoadToDatawarehouse, self).__init__(etl_table)

    def load_to_warehouse(self):
        self.load_to_warehouse(self.conf.get_data_db_info())

def arg_parse():
    parser = argparse.ArgumentParser(description='ETL Workflows')
    parser.add_argument('--directory', type=str, default='peer_assess_pro',
                        help='Workflow Directory')

    return parser

def extract_and_load(dirc):
    sword = Sword(dirc)
    # pap = PeerAssessPro(dirc)
    LoadToStaging(sword).load_to_warehouse()
    

if __name__ == '__main__':
    args = arg_parse().parse_args()
    dirc = args.directory
    extract_and_load(dirc)
