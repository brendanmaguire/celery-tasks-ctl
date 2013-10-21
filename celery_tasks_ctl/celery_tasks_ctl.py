#!/usr/bin/env python

'''
Celery Tasks Controller

List and revoke Celery tasks

Usage:
    celery_tasks_ctl list [options] (all | active | reserved | queued)
    celery_tasks_ctl revoke [options] (--regex=<TASK_REGEX> | [TASKID])
    celery_tasks_ctl -h | --help

    Options:
        -h --help                                       Print this help
        --hostname=<HOSTNAME>           \
                RabbitMQ Server [default: localhost]
        --port=<PORT>                   \
                RabbitMQ Port [default: 5672]
        --virtual-host=<VIRTUAL-HOST>   \
                RabbitMQ Virtual Host [default: /]
        --loglevel=(debug | info | warning | error)     [default: warning]
'''

import itertools
import json
import logging
import pika
import re

from celery import Celery
from docopt import docopt

CELERY_QUEUE_NAME = 'celery'

LOG = logging.getLogger(__name__)


def active_tasks(celery_inspect_obj):
    return list(extract_task_ids(celery_inspect_obj.active()))


def reserved_tasks(celery_inspect_obj):
    return list(extract_task_ids(celery_inspect_obj.reserved()))


def queued_tasks(mq_channel, queue):
    """
    Get task ids for all tasks in the specified queue

    :param mq_channel: The pika Channel to use to connect queue
    :param queue: The queue to read tasks from
    """
    msgs = []
    while True:
        # Get tasks off the queue until empty (i.e. None returned)
        method, header, body_str = mq_channel.basic_get(queue=queue)
        if method is None:
            break

        body = json.loads(body_str)
        msgs.append((method.delivery_tag, body['id']))

    for delivery_tag, task_id in msgs:
        # Reject the tasks, therefore putting them back on the queue
        mq_channel.basic_reject(delivery_tag=delivery_tag)

    return [task_id for _, task_id in msgs]


def extract_task_ids(inspect_map):
    """ Yields all task ids for the inspect map """
    if inspect_map:
        for instance, tasks in inspect_map.iteritems():
            for task in tasks:
                yield task['id']


def get_task_ids_map(task_types, mq_channel, celery):
    """
    Get a list of task ids from Celery and RabbitMQ

    :param task_types: A list of tasks to retrieve. Can be any subset of the
                       following: ['active', 'reserved', 'queued']
    :param mq_channel: The pika.channel.Channel object to get queued tasks with
    :param celery: The celery object to inspect the celery tasks with

    :return: {<task_type>: [task_ids],...}
    """
    inspect = celery.control.inspect()

    task_ids_map = {}
    if 'active' in task_types:
        task_ids_map['active'] = active_tasks(inspect)

    if 'reserved' in task_types:
        task_ids_map['reserved'] = reserved_tasks(inspect)

    if 'queued' in task_types:
        task_ids_map['queued'] = queued_tasks(mq_channel, CELERY_QUEUE_NAME)

    LOG.debug('Found tasks: %s', task_ids_map)

    return task_ids_map


def list_task_ids(task_types, mq_channel, celery):
    """
    Prints task ids and their type
    """
    task_ids_map = get_task_ids_map(task_types, mq_channel, celery)
    for task_type, task_ids in task_ids_map.iteritems():
        print_task_ids(task_ids, task_type)


def print_task_ids(tasks, heading):
    divider = '-' * 40
    print('* {0}\n{1}\n{2}'.format(heading, '\n'.join(tasks), divider))


def revoke_tasks_matching_regex(task_id_regex, mq_channel, celery):
    """
    Revoke all tasks currently been processed or waiting to be processed whos
    id match the specified regex
    """
    LOG.info('Revoking tasks that match regex: %s', task_id_regex)

    task_ids_map = get_task_ids_map(['active', 'reserved', 'queued'],
                                    mq_channel, celery)
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


def get_mq_channel(hostname, port, virtual_host):
    conn_params = pika.ConnectionParameters(host=hostname, port=port,
                                            virtual_host=virtual_host)
    conn = pika.BlockingConnection(conn_params)
    return conn.channel()


def get_celery_connection(hostname, port, virtual_host):
    broker_url = 'amqp://{0}:{1}/{2}'.format(hostname, port, virtual_host)
    return Celery(broker=broker_url)


def main(args):
    hostname = args['--hostname']
    port = int(args['--port'])
    virtual_host = args['--virtual-host']

    mq_channel = get_mq_channel(hostname, port, virtual_host)
    celery = get_celery_connection(hostname, port, virtual_host)

    if args['list']:
        task_types = ['active', 'reserved', 'queued']
        if not args['all']:
            task_types = [task_type for task_type in task_types
                          if args[task_type]]
        list_task_ids(task_types, mq_channel, celery)
    elif args['revoke']:
        task_id_regex = args.get('--regex')
        if task_id_regex:
            revoked_tasks = revoke_tasks_matching_regex(task_id_regex,
                                                        mq_channel, celery)
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
