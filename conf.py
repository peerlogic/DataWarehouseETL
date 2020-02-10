import json

class Conf(object):
    PATH = './ETL.json'
    def __init__(self):
        try:
            self.conf = json.load(open(Conf.PATH, 'rb'))
        except FileNotFoundError:
            raise "Please include the configuration file in this directory"

    def get_staging_db_info(self):
        return self.conf['staging_db']
