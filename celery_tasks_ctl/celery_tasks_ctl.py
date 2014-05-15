#!/usr/bin/env python

'''
Celery Tasks Controller

List and revoke Celery tasks

Usage:
    celery_tasks_ctl list [options] (all | active | reserved | queued)
    celery_tasks_ctl revoke [options] (--regex=<TASK_REGEX> | [TASKID])
    celery_tasks_ctl -h | --help
    celery_tasks_ctl --version

Options:
    --hostname=<HOSTNAME>               \
            RabbitMQ Server [default: localhost]
    --amqp-port=<PORT>                       \
            RabbitMQ AMQP Port [default: 5672]
    --api-port=<PORT>                       \
            RabbitMQ API Port [default: 15672]
    --virtual-host=<VIRTUAL-HOST>       \
            RabbitMQ Virtual Host [default: /]
    --username=<USERNAME>
            RabbitMQ API username [default: guest]
    --password=<PASSWORD>
            RabbitMQ API password [default: guest]
    --loglevel=(debug | info | warning | error)     [default: warning]
    -h --help                                       Print this help
    --version                                       Print the version
'''

import base64
import itertools
import json
import logging
import pickle
import re
import requests
import urllib

from celery import Celery
from collections import namedtuple
from docopt import docopt

from ._version import __version__

CELERY_QUEUE_NAME = 'celery'
_API_URL_TEMPLATE = ('http://{host}:{port}'
                     '/api/queues/{vhost}/{queue}/get')
_API_MAX_MESSAGE_COUNT = 10000

LOG = logging.getLogger(__name__)

HttpConnectionParams = namedtuple('HttpConnectionParams',
                                  ['host', 'port', 'vhost', 'queue', 'username', 'password'])


def queued_tasks(http_conn_params):
    """
    Get task ids for all tasks in the specified queue

    :param http_conn_params: An instance of HttpConnectionParams
    """
    url = _API_URL_TEMPLATE.format(
        host=http_conn_params.host,
        port=http_conn_params.port,
        vhost=urllib.quote_plus(http_conn_params.vhost),
        queue=urllib.quote_plus(http_conn_params.queue))

    post_data = json.dumps({'count': _API_MAX_MESSAGE_COUNT,
                            'requeue': True,
                            'encoding': 'base64'})
    auth = (http_conn_params.username, http_conn_params.password)
    response = requests.post(url, data=post_data, auth=auth)

    response.raise_for_status()

    return _extract_task_ids_from_messages(response.json())


def _extract_task_ids_from_messages(messages):
    task_ids = []

    for message in messages:
        payload_str = base64.b64decode(message['payload'])
        content_type = message['properties']['content_type']
        if content_type == 'application/x-python-serialize':
            body = pickle.loads(payload_str)
        elif content_type == 'application/json':
            body = json.loads(payload_str)
        else:
            raise NotImplementedError(
                'No decode logic for messages of type %s yet' %
                content_type)
        task_ids.append(body['id'])

    return task_ids


def active_tasks(celery_inspect_obj):
    return list(extract_task_ids(celery_inspect_obj.active()))


def reserved_tasks(celery_inspect_obj):
    return list(extract_task_ids(celery_inspect_obj.reserved()))


def extract_task_ids(inspect_map):
    """ Yields all task ids for the inspect map """
    if inspect_map:
        for instance, tasks in inspect_map.iteritems():
            for task in tasks:
                yield task['id']


def get_task_ids_map(task_types, http_conn_params, celery):
    """
    Get a list of task ids from Celery and RabbitMQ

    :param task_types:
        A list of tasks to retrieve. Can be any subset of the following:
        ['active', 'reserved', 'queued']
    :param http_conn_params:
        An instance of HttpConnectionParams
    :param celery:
        The celery object to inspect the celery tasks with

    :return: {<task_type>: [task_ids],...}
    """
    inspect = celery.control.inspect()

    task_ids_map = {}
    if 'active' in task_types:
        task_ids_map['active'] = active_tasks(inspect)

    if 'reserved' in task_types:
        task_ids_map['reserved'] = reserved_tasks(inspect)

    if 'queued' in task_types:
        task_ids_map['queued'] = queued_tasks(http_conn_params)

    LOG.debug('Found tasks: %s', task_ids_map)

    return task_ids_map


def list_task_ids(task_types, http_conn_params, celery):
    """
    Prints task ids and their type
    """
    task_ids_map = get_task_ids_map(task_types, http_conn_params, celery)
    for task_type, task_ids in task_ids_map.iteritems():
        print_task_ids(task_ids, task_type)


def print_task_ids(tasks, heading):
    divider = '-' * 40
    print('* {0}\n{1}\n{2}'.format(heading, '\n'.join(tasks), divider))


def revoke_tasks_matching_regex(task_id_regex, http_conn_params, celery):
    """
    Revoke all tasks currently been processed or waiting to be processed whos
    id match the specified regex
    """
    LOG.info('Revoking tasks that match regex: %s', task_id_regex)

    task_ids_map = get_task_ids_map(['active', 'reserved', 'queued'],
                                    http_conn_params, celery)
    task_ids = list(itertools.chain.from_iterable(task_ids_map.values()))

    matching_task_ids = filter(
        lambda task_id: re.search(r'%s' % task_id_regex, task_id),
        task_ids)
    LOG.debug('Matching tasks: %s', matching_task_ids)

    for task_id in matching_task_ids:
        revoke_task(task_id, celery)

    return matching_task_ids


def revoke_task(task_id, celery):
    LOG.info('Revoking task: %s', task_id)
    celery.control.revoke(task_id, terminate=True)


def get_celery_connection(hostname, port, virtual_host):
    broker_url = 'amqp://{0}:{1}/{2}'.format(hostname, port, virtual_host)
    return Celery(broker=broker_url)


def main(args):
    if args['--version']:
        print 'Version %s' % __version__
        return 0

    hostname = args['--hostname']
    amqp_port = int(args['--amqp-port'])
    api_port = int(args['--api-port'])
    virtual_host = args['--virtual-host']
    username = args['--username']
    password = args['--password']

    http_conn_params = HttpConnectionParams(
        hostname, api_port, virtual_host, CELERY_QUEUE_NAME, username,
        password)

    celery = get_celery_connection(hostname, amqp_port, virtual_host)

    if args['list']:
        task_types = ['active', 'reserved', 'queued']
        if not args['all']:
            task_types = [task_type for task_type in task_types
                          if args[task_type]]
        list_task_ids(task_types, http_conn_params, celery)
    elif args['revoke']:
        task_id_regex = args.get('--regex')
        if task_id_regex:
            revoked_tasks = revoke_tasks_matching_regex(task_id_regex,
                                                        http_conn_params, celery)
            if revoked_tasks:
                print('Revoked the following tasks:\n{0}'.format(
                    '\n'.join(revoked_tasks)))
            else:
                print('No tasks matched regex')
        else:
            revoke_task(args['TASKID'], celery)


if __name__ == '__main__':
    logging.basicConfig()
    LOG = logging.getLogger('celery_task_ctl')

    args = docopt(__doc__)

    # Set the log level if called as a script
    LOG.setLevel(getattr(logging, args['--loglevel'].upper()))

    exit(main(args))
