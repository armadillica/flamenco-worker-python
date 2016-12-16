# -*- encoding: utf-8 -*-
from __future__ import absolute_import

from pillar.tests import common_test_data as ctd
from abstract_flamenco_test import AbstractFlamencoTest


class TaskManagerTest(AbstractFlamencoTest):
    def test_create_task(self):
        from pillar.api.utils.authentication import force_cli_user
        from flamenco.job_compilers import commands

        manager, _, _ = self.create_manager_service_account()

        with self.app.test_request_context():
            force_cli_user()
            job_doc = self.jmngr.api_create_job(
                'test job',
                u'Wörk wørk w°rk.',
                'sleep', {
                    'frames': '12-18, 20-22',
                    'chunk_size': 7,
                    'time_in_seconds': 3,
                },
                self.proj_id,
                ctd.EXAMPLE_PROJECT_OWNER_ID,
                manager['_id'],
            )

            self.tmngr.api_create_task(
                job_doc,
                [
                    commands.Echo(message=u'ẑžƶźz'),
                    commands.Sleep(time_in_seconds=3),
                ],
                'sleep-1-13',
            ),

        # Now test the database contents.
        with self.app.test_request_context():
            tasks_coll = self.flamenco.db('tasks')
            dbtasks = list(tasks_coll.find())
            self.assertEqual(3, len(dbtasks))  # 2 of compiled job + the one we added after.

            dbtask = dbtasks[-1]

            self.assertEqual({
                u'name': u'echo',
                u'settings': {
                    u'message': u'ẑžƶźz',
                }
            }, dbtask['commands'][0])

            self.assertEqual({
                u'name': u'sleep',
                u'settings': {
                    u'time_in_seconds': 3,
                }
            }, dbtask['commands'][1])
