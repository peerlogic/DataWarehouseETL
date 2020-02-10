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
            .fromcsv(f'{self._dirc}/all_users.csv', delimiter=';')
            .convert('user_id', 'id')
            .select(lambda row: row.research == 'Y')
            .cut('id')
        )

    def _get_actors(self):
        # TODO: Deal with both
        student_actors = (
            etl
            .fromcsv(f'{self._dirc}/all_users.csv', delimiter=';')
            .select(lambda row: row.role == 'STUDENT')
            .cut('id')
            .addcolumn('role', [], missing='Student')
        )

        teacher_actors = (
            etl
            .fromcsv(f'{self._dirc}/all_users.csv', delimiter=';')
            .select(lambda row: row.role == 'TEACHER')
            .cut('id')
            .addcolumn('role', [], missing='Teacher')
        )

        return etl.cat(student_actors, teacher_actors) 

    def _get_actor_pariticipants(self):
        # wait to clarify issue in get_actors
        student_actors = (
            etl
            .fromcsv(f'{self._dirc}/student.csv', delimiter=';')
            .cut('id', 'id')
            .rename(0, 'actor_id')
            .rename(1, 'participant_id')
        )

        teacher_actors = (
            etl
            .fromcsv(f'{self._dirc}/teacher.csv', delimiter=';')
            .cut('id', 'id')
            .rename(0, 'actor_id')
            .rename(1, 'participant_id')
        )

        return etl.cat(student_actors, teacher_actors)

    def _get_tasks(self):
        date = etl.datetimeparser('%m/%d/%Y %H:%M')
        return (
            etl
            .fromcsv(f'{self._dirc}/all_courses.csv', delimiter=';')
            .convert('course_id', 'id')
            .convert('courseName', 'course_title')
            .convert('university', 'organization_title')
            .convert('creationDate', 'starts_at')
            .convert('creationDate', date)
            .createdBy
        )

    def _get_criteria(self):
        # TODO: only has criteria titles and no IDs
        return (
            etl
            .fromcolumns([
                self._get_imported_criteria_list(), self._get_imported_criteria_list()
            ])
            .rename(0, 'id')
            .rename(1, 'title')
        )       

    def _get_answers(self):
        comments = (
            etl
            .fromcsv(f'{self._dirc}/assessment_result.csv', delimiter=';')
            .listoflists()
        )

    def _get_artifacts(self):
        return None

    def _get_eval_modes(self):
        return None

    def _get_items(self):
        return None