from etl_table import ETLTable
import petl as etl
from conf import Conf

class DatabaseExtract(ETLTable):

    def __init__(self):
        self.conf = Conf()
        self.db_info = self.conf.get_staging_db_info()
        self.connection = pymysql.connect(
            host = self.db_info['host'], user = self.db_info['user'],
            password = self.db_info['passwd'], db = self.db_info['db'],
        )

    def _get_actor_particpants(self):
        return etl.fromdb(self.connection, 'SELECT * FROM actor_participants')

    def _get_actors(self):
        return etl.fromdb(self.connection, 'SELECT * FROM actors')

    def _get_answers(self):
        return etl.fromdb(self.connection, 'SELECT * FROM answers')

    def _get_artifacts(self):
        return etl.fromdb(self.connection, 'SELECT * FROM artifacts')

    def _get_criteria(self):
        return etl.fromdb(self.connection, 'SELECT * FROM criteria')

    def _get_eval_modes(self):
        return etl.fromdb(self.connection, 'SELECT * FROM eval_modes')

    def _get_items(self):
        return etl.fromdb(self.connection, 'SELECT * FROM items')

    def _get_participants(self):
        return etl.fromdb(self.connection, 'SELECT * FROM participants')

    def _get_tasks(self):
        return etl.fromdb(self.connection, 'SELECT * FROM tasks')