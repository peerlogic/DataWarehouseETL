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

# TODO: We should be able to grab this information from the database instead of hardcoding
COLUMNS = {
    ACTOR_PARTICIPANTS: ['actor_id', 'participant_id'],
    ACTORS: ['id', 'role'],
    ANSWERS: [
        'id', 'assessor_actor_id', 'assessee_actor_id', 'assessee_artifact_id', 
        'criterion_id', 'evaluation_mode_id', 'comment', 'comment2', 'rank', 'score',
        'create_in_task_id', 'submitted_at',
    ],
    CRITERIA: ['id', 'title', 'description', 'type', 'min_score', 'max_score', 'weight'],
    EVAL_MODES: ['id', 'description'],
    ITEMS: ['id', 'content', 'reference_id', 'type'],
    PARTICIPANTS: ['id', 'app_name'],
    TASKS: [
        'id', 'task_type', 'task_description', 'starts_at', 'ends_at', 'assignment_title', 
        'course_title', 'organization_title', 'owner_name', 'cip_level1_code', 
        'cip_level2_code', 'cip_level3_code', 'app_name',
    ],
    ARTIFACTS: ['id', 'content', 'elaboration', 'submitted_in_task_id', 'context_case']
}

def _add_missing_columns(table, columns):
    for c in columns:
        if c not in table.columns():
            table = table.addcolumn(c, [], missing=None)
    
    return table

class ETLTable(object):
    # TODO: Fix error: pymysql.err.IntegrityError: (1451, 'Cannot delete or update a parent row: a foreign key constraint fails ("staging_warehouse"."actor_participants", CONSTRAINT "actor_participant_participant_id" FOREIGN KEY ("participant_id") REFERENCES "participants" ("id") ON DELETE NO ACTION ON UPDATE NO ACTION)'
    # Cannot handle uploading old data, should force overwrite
    
    def __init__(self, app_name, abbrev=None):
        self.app_name = app_name
        self.abbrev = abbrev if abbrev else self.app_name[:4]
        # TODO: It should be possible to grab this from the schema
        self._convert_id = lambda r: self.abbrev + '-' + '0' * (8 - len(r)) + r if r!='None' else None
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

    def get_actor_pariticipants(self):
        return (
            _add_missing_columns(
                self._get_actor_pariticipants(), COLUMNS[ACTOR_PARTICIPANTS]
            ) 
            .convert('actor_id', str)
            .convert('participant_id', str)
            .convert('actor_id', self._convert_id)
            .convert('participant_id', self._convert_id)
        )
    
    def get_actors(self):
        return (
            _add_missing_columns(
                self._get_actors(), COLUMNS[ACTORS]
            )
            .convert('id', str)
            .convert('role', str)
            .convert('id', self._convert_id)
        )
    
    def get_answers(self):
        return (
            _add_missing_columns(
                self._get_answers(), COLUMNS[ANSWERS]
            )
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
        return _add_missing_columns(
            self._get_artifacts(), COLUMNS[ARTIFACTS]
        )
    
    def get_criteria(self):
        return (
            _add_missing_columns(
                self._get_criteria(), COLUMNS[CRITERIA]
            )
            .convert('id', str)
            .convert('title', str)
            .convert('id', self._convert_id)
        )
    
    def get_eval_modes(self):
        return _add_missing_columns(
            self._get_eval_modes(), COLUMNS[EVAL_MODES]
        )

    def get_items(self):
        return _add_missing_columns(
            self._get_items(), COLUMNS[ITEMS]
        )
    
    def get_participants(self):
        return (
            _add_missing_columns(
                self._get_participants(), COLUMNS[PARTICIPANTS]
            )
            .addcolumn('app_name', [], missing=self.app_name)
            .convert('id', str)
            .convert('app_name', str)
            .convert('id', self._convert_id)
        )
    
    def get_tasks(self):
        return _add_missing_columns(
            self._get_tasks(), COLUMNS[TASKS]
        )

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