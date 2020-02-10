import pymysql
import petl as etl
from conf import Conf

ACTOR_PARTICIPANTS = 'actor_participants'
ACTORS = 'actors'
ANSWERS = 'answers'
ARTIFACTS = 'artifacts'
CRITERIA = 'criteria'
EVAL_MODES = 'eval_modes'
ITEMS = 'items'
PARTICIPANTS = 'participants'
TASKS = 'tasks'

class ETLTable(object):
    # TODO: Fix error: pymysql.err.IntegrityError: (1451, 'Cannot delete or update a parent row: a foreign key constraint fails ("staging_warehouse"."actor_participants", CONSTRAINT "actor_participant_participant_id" FOREIGN KEY ("participant_id") REFERENCES "participants" ("id") ON DELETE NO ACTION ON UPDATE NO ACTION)'
    # Cannot handle uploading old data, can handle that loading from staging to data warehouse

    def __init__(self, app_name, abbrev=None):
        self.app_name = app_name
        self.abbrev = abbrev if abbrev else self.app_name[:4]
        self.conf = Conf()
        self.TABLES = {
            ACTOR_PARTICIPANTS: self.get_actor_pariticipants,
            ACTORS: self.get_actors,
            ANSWERS: self.get_answers,
            ARTIFACTS: self.get_artifacts,
            CRITERIA: self.get_criteria,
            EVAL_MODES: self.get_eval_modes,
            ITEMS: self.get_items,
            PARTICIPANTS: self.get_participants,
            TASKS: self.get_tasks,
        }
        # TODO: It should be possible to grab this from the schema
        self.UPDATE_ORDER = [
            PARTICIPANTS, ACTORS, ACTOR_PARTICIPANTS, CRITERIA, EVAL_MODES,
            TASKS, ITEMS, ARTIFACTS, ANSWERS,
        ]

        self._convert_id = lambda r: self.abbrev + '-' + '0' * (8 - len(r)) + r if r else None
    
    def load_to_staging_warehouse(self):
        db_info = self.conf.get_staging_db_info()
        connection = pymysql.connect(
            host=db_info['host'], user=db_info['user'], 
            password=db_info['passwd'], db=db_info['db'],
        )
        connection.cursor().execute('SET SQL_MODE=ANSI_QUOTES')
        # TODO: ask Yang about peerlogic set up, why is there an error for duplicate
        # entries in actor IDs, can't different roles share primary keys for IDs
        for table in self.UPDATE_ORDER:
            data = self.TABLES[table]()
            if data:
                print(f'Loading {table}...\n{self.TABLES[table]()}')
                etl.todb(data, connection, table)
        connection.close()

    def get_actor_pariticipants(self):
        return (
            self._get_actor_pariticipants()
            .convert('actor_id', str)
            .convert('participant_id', str)
            .convert('actor_id', self._convert_id)
            .convert('participant_id', self._convert_id)
        )
    
    def get_actors(self):
        return (
            self._get_actors()
            .convert('id', str)
            .convert('role', str)
            .convert('id', self._convert_id)
        )
    
    def get_answers(self):
        return (
            self._get_answers()
            .convert('id', str)
            .convert('assessee_artifact_id', str)
            .convert('assessee_actor_id', str)
            .convert('assessor_actor_id', str)
            .convert('comment', str)
            .convert('criterion_id', str)
            .convert('id', self._convert_id)
            .convert('assessor_actor_id', self._convert_id)
            .convert('assessee_actor_id', self._convert_id)
            .convert('assessee_artifact_id', self._convert_id)
            .convert('criterion_id', self._convert_id)
        )
    
    def get_artifacts(self):
        return self._get_artifacts()
    
    def get_criteria(self):
        return (
            self._get_criteria()
            .convert('id', str)
            .convert('title', str)
            .convert('id', self._convert_id)
        )
    
    def get_eval_modes(self):
        return self._get_eval_modes()

    def get_items(self):
        return self._get_items()
    
    def get_participants(self):
        return (
            self._get_participants()
            .addcolumn('app_name', [], missing=self.app_name)
            .convert('id', str)
            .convert('app_name', str)
            .convert('id', self._convert_id)
        )
    
    def get_tasks(self):
        return self._get_tasks()

    def _get_actor_pariticipants(self):
        raise UnImplementedMethodError()

    def _get_actors(self):
        raise UnImplementedMethodError()

    def _get_answers(self):
        raise UnImplementedMethodError()

    def _get_artifacts(self):
        raise UnImplementedMethodError()

    def _get_criteria(self):
        raise UnImplementedMethodError()

    def _get_eval_modes(self):
        raise UnImplementedMethodError()

    def _get_items(self):
        raise UnImplementedMethodError()

    def _get_participants(self):
        raise UnImplementedMethodError()

    def _get_tasks(self):
        raise UnImplementedMethodError()

class UnImplementedMethodError(Exception):
    pass