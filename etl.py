import argparse
import pymysql
from peer_assess_pro import PeerAssessPro
from sword import Sword

ACTOR_PARTICIPANTS = 'actor_participants'
ACTORS = 'actors'
ANSWERS = 'answers'
ARTIFACTS = 'artifacts'
CRITERIA = 'criteria'
EVAL_MODES = 'eval_modes'
ITEMS = 'items'
PARTICIPANTS = 'participants'
TASKS = 'tasks'

class LoadToDatabase(object):
    def __init__(self, etl_table):
        self.conf = Conf()
        self.etl_table = etl_table
        self.TABLES = {
            ACTOR_PARTICIPANTS: self.etl_table.get_actor_pariticipants,
            ACTORS: self.etl_table.get_actors,
            ANSWERS: self.etl_table.get_answers,
            ARTIFACTS: self.etl_table.get_artifacts,
            CRITERIA: self.etl_table.get_criteria,
            EVAL_MODES: self.etl_table.get_eval_modes,
            ITEMS: self.etl_table.get_items,
            PARTICIPANTS: self.etl_table.get_participants,
            TASKS: self.etl_table.get_tasks,
        }
        # TODO: It should be possible to grab this from the schema
        self.UPDATE_ORDER = [
            PARTICIPANTS, ACTORS, ACTOR_PARTICIPANTS, CRITERIA, EVAL_MODES,dffff
            TASKS, ITEMS, ARTIFACTS, ANSWERS,
        ]

    def load_to_warehouse(self, db_info):
        connection = pymysql.connect(
            host=db_info['host'], user=db_info['user'],
            password=db_info['passwd'], db=db_info['db'],
        )
        connection.cursor().execute('SET SQL_MODE=ANSI_QUOTES')
        for table in self.UPDATE_ORDER:
            data = self.TABLES[table]()
            print(table)
            if data:
                print(f'Loading {table}...\n{self.TABLES[table]()}')
                etl.todb(data, connection, table)
        connection.close()

class LoadToStaging(LoadToDatabase):
    def __init__(self, etl_table):
        super().__init__(self, etl_table)

    def load_to_warehouse(self):
        super().load_to_warehouse(self.conf.get_staging_db_info())

class LoadToDatawarehouse(LoadToDatabase):
    def __init__(self, etl_table):
        super().__init__(self, etl_table)

    def load_to_warehouse(self):
        super().load_to_warehouse(self.conf.get_data_db_info())

def arg_parse():
    parser = argparse.ArgumentParser(description='ETL Workflows')
    parser.add_argument('--directory', type=str, default='peer_assess_pro',
                        help='Workflow Directory')

    return parser

def extract_and_load(dirc):
    # sword = Sword(dirc)
    # sword.load_to_staging_warehouse()
    pap = PeerAssessPro(dirc)
    pap.load_to_staging_warehouse()
    

if __name__ == '__main__':
    args = arg_parse().parse_args()
    dirc = args.directory
    extract_and_load(dirc)
