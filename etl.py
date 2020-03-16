import argparse
import pymysql
from peer_assess_pro import PeerAssessPro
from etl_table import ETLTable

def load_to_staging_warehouse(self):
    db_info = self.conf.get_staging_db_info()
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

def arg_parse():
    parser = argparse.ArgumentParser(description='ETL Workflows')
    parser.add_argument('--directory', type=str, default='peer_assess_pro',
                        help='Workflow Directory')

    return parser

def extract_and_load(dirc):
    pap = PeerAssessPro(dirc)
    pap.load_to_staging_warehouse()
    

if __name__ == '__main__':
    args = arg_parse().parse_args()
    dirc = args.directory
    extract_and_load(dirc)
