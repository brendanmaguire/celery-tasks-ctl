import unittest

from celery_tasks_ctl.celery_tasks_ctl import extract_task_ids


class TestCeleryTasksCtl(unittest.TestCase):
    def test_extract_task_ids_simple(self):
        inspect_map = {
            u'hostname1': [
                {u'args': u'[60]',
                 u'time_start': 1382351060.761985,
                 u'name': u'staller.tasks.stall',
                 u'delivery_info': {
                     u'priority': None,
                     u'routing_key': u'celery',
                     u'exchange': u'celery'},
                 u'hostname': u'hostname1',
                 u'acknowledged': True,
                 u'kwargs': u'{}',
                 u'id': u'whisper18',
                 u'worker_pid': 21032},
                {u'args': u'[60]',
                 u'time_start': 1382351060.762472,
                 u'name': u'staller.tasks.stall',
                 u'delivery_info': {
                    u'priority': None,
                    u'routing_key': u'celery',
                    u'exchange': u'celery'},
                 u'hostname': u'hostname1',
                 u'acknowledged': True,
                 u'kwargs': u'{}',
                 u'id': u'whisper21',
                 u'worker_pid': 32686},
                {u'args': u'[60]',
                 u'time_start': 1382351060.761889,
                 u'name': u'staller.tasks.stall',
                 u'delivery_info': {
                    u'priority': None,
                    u'routing_key': u'celery',
                    u'exchange': u'celery'},
                 u'hostname': u'hostname1',
                 u'acknowledged': True,
                 u'kwargs': u'{}',
                 u'id': u'whisper17',
                 u'worker_pid': 19323},
                {u'args': u'[60]',
                 u'time_start': 1382351060.762128,
                 u'name': u'staller.tasks.stall',
                 u'delivery_info': {
                     u'priority': None,
                     u'routing_key': u'celery',
                     u'exchange': u'celery'},
                 u'hostname': u'hostname1',
                 u'acknowledged': True,
                 u'kwargs': u'{}',
                 u'id': u'whisper19',
                 u'worker_pid': 21233}
            ]
        }
        task_ids = extract_task_ids(inspect_map)
        self.assertEqual(['whisper17', 'whisper18', 'whisper19', 'whisper21'],
                         sorted(list(task_ids)))

    def test_extract_task_ids_two_hosts(self):
        inspect_map = {
            u'hostname1': [
                {u'args': u'[60]',
                 u'time_start': 1382351060.761985,
                 u'name': u'staller.tasks.stall',
                 u'delivery_info': {
                     u'priority': None,
                     u'routing_key': u'celery',
                     u'exchange': u'celery'},
                 u'hostname': u'hostname1',
                 u'acknowledged': True,
                 u'kwargs': u'{}',
                 u'id': u'whisper18',
                 u'worker_pid': 21032}
            ],
            u'hostname2': [
                {u'args': u'[60]',
                 u'time_start': 1382351060.762472,
                 u'name': u'staller.tasks.stall',
                 u'delivery_info': {
                    u'priority': None,
                    u'routing_key': u'celery',
                    u'exchange': u'celery'},
                 u'hostname': u'hostname1',
                 u'acknowledged': True,
                 u'kwargs': u'{}',
                 u'id': u'whisper21',
                 u'worker_pid': 32686},
            ]
        }
        task_ids = extract_task_ids(inspect_map)
        self.assertEqual(['whisper18', 'whisper21'],
                         sorted(list(task_ids)))

    def test_extract_task_ids_none(self):
        task_ids = extract_task_ids(None)
        self.assertEqual([], list(task_ids))
