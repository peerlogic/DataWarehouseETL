from etl_table import ETLTable
import petl as etl
import numpy as np

class Sword(ETLTable):
    def __init__(self, dirc):
        super().__init__('Sword', 'SW')
        self._dirc = dirc

    def _get_participants(self):
        return (
            etl
            .fromcsv(f'{self._dirc}/all_users.csv', delimiter=',')
            .rename('user_id', 'id')
            .select(lambda row: row.research == 'Y')
            .cut('id')
        )

    def _get_actors(self):
        student_actors = (
            etl
            .fromcsv(f'{self._dirc}/all_users.csv', delimiter=',')
            .select(lambda row: row.role == 'STUDENT' or row.role == 'BOTH')
            .rename('user_id', 'id')
            .cut('id')
            .addcolumn('role', [], missing='student')
        )

        teacher_actors = (
            etl
            .fromcsv(f'{self._dirc}/all_users.csv', delimiter=',')
            .select(lambda row: row.role == 'TEACHER')
            .rename('user_id', 'id')
            .cut('id')
            .addcolumn('role', [], missing='instructor')
        )

        return etl.cat(student_actors, teacher_actors) 

    def _get_actor_pariticipants(self):
        # wait to clarify issue in get_actors
        all_actors = (
            etl
            .fromcsv(f'{self._dirc}/all_users.csv', delimiter=',')
            .cut('user_id', 'user_id')
            .rename(0, 'actor_id')
            .rename(1, 'participant_id')
        )

        return all_actors

    def _get_tasks(self):
        # TODO: should owner_name be the instructor id?
        # yes it can be
        date = etl.datetimeparser('%m/%d/%Y %H:%M')
        return (
            etl
            .fromcsv(f'{self._dirc}/all_courses.csv', delimiter=',')
            .rename('course_id', 'id')
            .rename('courseName', 'course_title')
            .rename('university', 'organization_title')
            .rename('creationDate', 'starts_at')
            .convert('starts_at', date)
            .addcolumn('app_name', [], missing=self.abbrev)
            .rename('createdBy', 'owner_name')
            .cut('id', 'course_title', 'organization_title', 'starts_at', 'app_name', 'owner_name')
        )

    def _get_criteria(self):
        # TODO: only has criteria titles and no IDs
        criteria_ids = (
            etl
            .fromcsv(f'{self._dirc}/rawCourseRatingsData_1.csv', delimiter=';')
            .cat(etl.fromcsv(f'{self._dirc}/rawCourseRatingsData_2.csv', delimiter=';'))
            .cut('Dimension ID')
            .rename('Dimension ID', 'id')
        )
        return etl.fromcolumns(list(criteria_ids)) 

    def _get_answers(self):
        comments = (
            etl
            .fromcsv(f'{self._dirc}/assessment_result.csv', delimiter=',')
            .listoflists()
        )

    def _get_artifacts(self):
        return etl.unpack([])

    def _get_eval_modes(self):
        return etl.unpack([])

    def _get_items(self):
        return etl.unpack([])