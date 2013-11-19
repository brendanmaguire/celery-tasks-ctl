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
    --port=<PORT>                       \
            RabbitMQ Port [default: 5672]
    --virtual-host=<VIRTUAL-HOST>       \
            RabbitMQ Virtual Host [default: /]
    --loglevel=(debug | info | warning | error)     [default: warning]
    -h --help                                       Print this help
    --version                                       Print the version
'''

import itertools
import json
import logging
import pickle
import pika
import re

from celery import Celery
from docopt import docopt

from ._version import __version__

CELERY_QUEUE_NAME = 'celery'

LOG = logging.getLogger(__name__)


def queued_tasks(mq_channel, queue):
    """
    Get task ids for all tasks in the specified queue

    :param mq_channel: The pika Channel to use to connect queue
    :param queue: The queue to read tasks from
    """
    amqp_messages = _read_messages_from_amqp(mq_channel, queue)
    return _extract_task_ids_from_amqp_messages(amqp_messages)


def _read_messages_from_amqp(mq_channel, queue):
    """ Reads all amqp messages from the queue and requeues them """
    amqp_messages = []
    delivery_tags = []
    try:
        # Wide try block to ensure no matter what happens in here we put
        # the messages we've taken back on to the queue
        while True:
            # Get tasks off the queue until empty (i.e. None returned)
            amqp_message = mq_channel.basic_get(queue=queue)
            method, _header, _body_str = amqp_message
            if method is None:
                break
            delivery_tags.append(method.delivery_tag)
            amqp_messages.append(amqp_message)
    finally:
        for delivery_tag in delivery_tags:
            # Reject the tasks, therefore putting them back on the queue
            mq_channel.basic_reject(delivery_tag=delivery_tag)

    return amqp_messages


def _extract_task_ids_from_amqp_messages(amqp_messages):
    task_ids = []

    for (method, header, body_str) in amqp_messages:
        if header.content_type == 'application/x-python-serialize':
            body = pickle.loads(body_str)
        elif header.content_type == 'application/json':
            body = json.loads(body_str)
        else:
            raise NotImplementedError(
                'No decode logic for messages of type %s yet' %
                header.content_type)
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
    if args['--version']:
        print 'Version %s' % __version__
        return 0

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
