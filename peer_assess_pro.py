from etl_table import ETLTable
import petl as etl
import numpy as np

class PeerAssessPro(ETLTable):
    def __init__(self, dirc):
        super().__init__('PeerAssessPro', 'PAP')
        self._dirc = dirc

    def _get_actor_pariticipants(self):
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

        # team_user_actors = (
        #     etl
        #     .fromcsv(f'{self._dirc}/team_allocation.csv', delimiter=';')
        #     .cut('team_id', 'student_id')
        #     .rename('team_id', 'actor_id')
        #     .convert('actor_id', str)
        #     .rename('student_id', 'participant_id')
        #     .convert('participant_id', str)
        # )
        return etl.cat(student_actors, teacher_actors)

    def _get_actors(self):
        student_actors = (
            etl
            .fromcsv(f'{self._dirc}/student.csv', delimiter=';')
            .cut('id')
            .addcolumn('role', [], missing='Student')
        )

        teacher_actors = (
            etl
            .fromcsv(f'{self._dirc}/teacher.csv', delimiter=';')
            .cut('id')
            .addcolumn('role', [], missing='Teacher')
        )

        # team_user_actors = (
        #     etl
        #     .fromcsv(f'{self._dirc}/team_allocation.csv', delimiter=';')
        #     .cut('team_id')
        #     .rename('team_id', 'id')
        #     .convert('id', str)
        #     .addcolumn('role', [], missing='Team')
        #     .convert('role', str)
        # )

        return etl.cat(student_actors, teacher_actors)        

    def _get_answers(self):
        # TODO: double check that assessee and assessor mixed up
        # confusion on assessee and assessor ids here, is assessment result peer feedback?
        assessment_results = (
            etl
            .fromcsv(f'{self._dirc}/assessment_result.csv', delimiter=';')
            .listoflists()
        )[1:]
        # create a separate row for each criteria
        # peer assess pro table format has each criteria as a separate column
        table = []
        for row in assessment_results:
            crit_names = self._get_imported_criteria_list()
            for i, answers in enumerate(zip(row[3:], crit_names)):
                comment, criteria = answers
                table.append(
                    [int(row[0]) * len(crit_names) + i] + row[1:3] + [comment, criteria]
                )

        assessments = (
            etl
            .fromcsv(f'{self._dirc}/assessment.csv', delimiter=';')
            .cut('id', 'assessor_id')
            .rename('id', 'assessee_artifact_id')
            .rename('assessor_id', 'assessee_actor_id') # also need to double check
        )

        return (
            etl
            .fromcolumns(np.array(table).T.tolist())
            .rename(0, 'id')
            .rename(1, 'assessee_artifact_id')
            .rename(2, 'assessor_actor_id') # should double check on this, i think assessee should be assessor on this ish
            .rename(3, 'comment')
            .rename(4, 'criterion_id')
            .leftjoin(assessments, key='assessee_artifact_id')
            .convert('assessee_artifact_id', lambda r: None)
        )

    def _get_artifacts(self):
        # TODO: double check that this should be empty
        # TODO: see if there is any point in keeping teacher adv
        return None

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

    def _get_eval_modes(self):
        return None

    def _get_items(self):
        return None

    def _get_participants(self):
        student_actors = (
            etl
            .fromcsv(f'{self._dirc}/student.csv', delimiter=';')
            .cut('id')
        )

        teacher_actors = (
            etl
            .fromcsv(f'{self._dirc}/teacher.csv', delimiter=';')
            .cut('id')
        )
        return etl.cat(student_actors, teacher_actors)

    def _get_tasks(self):
        return None

    def _get_imported_criteria_list(self):
        return list(
            etl
            .fromcsv(f'{self._dirc}/assessment_result.csv', delimiter=';')
            .cutout('id', 'assessment_id', 'assessee_id')
            .columns()
            .keys()
        )