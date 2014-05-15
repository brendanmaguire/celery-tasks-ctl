Overview
========

Command line utility to list and revoke `Celery <http://www.celeryproject.org/>`_ tasks

* Facilitates listing tasks that have not yet been reserved by a Celery instance and are still on the RabbitMQ queue.
* Revoke tasks where their ids match your regex

How it works
------------

It uses the Celery inspect object to get a list of statistics for the tasks currently running in, or reserved by, the cluster. Queued tasks are tasks that the cluster does not yet know about and are currently in the RabbitMQ queue. To get a list of these, the RabbitMQ API is used.

Install
=======
::

    python setup.py install

or

::

    pip install celery-tasks-ctl

Run
===
::

    celery_tasks_ctl --help

License
_______
Licensed under `MPL 1.1 <http://www.mozilla.org/MPL/1.1/>`_.
