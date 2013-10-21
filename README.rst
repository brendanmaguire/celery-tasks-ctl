Overview
========

Command line utility to list and revoke `Celery <http://www.celeryproject.org/>`_ tasks

* Facilitates listing tasks that have not yet been reserved by a Celery instance and are still on the RabbitMQ queue.
* Revoke tasks where their ids match your regex

Install
=======
::

    python setup.py install

Run
===
::

    celery_tasks_ctl --help

License
_______
Licensed under `MPL 1.1 <http://www.mozilla.org/MPL/1.1/>`_.
