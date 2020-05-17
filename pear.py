from etl_table import ETLTable
import petl as etl

class Pear(ETLTable):
    def __init__(self, dirc):
        super().__init__('Pear', 'PAR')
        self._dirc = dirc

    def _get_dump(self):
        return etl.fromcsv(f'{self.dirc}/pear_data.csv')

    def _get_actor_pariticipants(self):
        return (
            self.
            _get_actors()
            .cut('id', 'id')
            .rename(0, 'actor_id')
            .rename(1, 'participant_id')
        )

    def _get_actors(self):
        instructors = (
            self
            ._get_dump()
            .cut('INSTRUCTOR_InstructorID')
            .rename('INSTRUCTOR_InstructorID', 'id')
            .addcolumn('role', [], missing='instructor')
        )

        ratees = (
            self
            ._get_dump()
            .cut('md5(SCORE.Ratee_Email)')
            .rename('md5(SCORE.Ratee_Email)', 'id')
            .addcolumn('role', [], missing='student')
        )

        raters = (
            self
            ._get_dump()
            .cut('md5(SCORE.Rater_Email)')
            .rename('md5(SCORE.Rater_Email)', 'id')
            .addcolumn('role', [], missing='student')
        )

        return etl.cat(instructors, ratees, raters).


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