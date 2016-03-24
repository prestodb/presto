===================
Queue Configuration
===================

The queueing rules are defined in a JSON file and control the number of queries
that can be submitted to Presto and the quota of running queries per queue.
The filename of the JSON config file should be specified in ``query.queue-config-file``
config property.

Rules that specify multiple queues will cause the query to acquire the queues'
permits sequentially. The query must acquire all queues' permits before it starts
being executed. It acquires the next queue permit only after it is accepted for
execution by the previous queue. A slot for the query is reserved in all queues.
The query is rejected if no slot is available in any of the queues.

Rules are processed sequentially and the first one that matches will be used.
In the example configuration below, there are four queue templates.
In the ``user.${USER}`` queue, ``${USER}`` will be expanded to the name of the
user that submitted the query. ``${SOURCE}`` is also supported, which expands
to the source submitting the query. The source name can be set as follows:

  * CLI: use the ``--source`` option.

  * JDBC: set the ``ApplicationName`` client info property on the ``Connection`` instance.

There are three rules that define which queries go into which queues:

  * The first rule makes ``bob`` an admin.

  * The second rule states that all queries that come from a source that includes ``pipeline``
    should first be queued in the user's personal queue, then the ``pipeline`` queue. When a
    query acquires a permit from a new queue, it doesn't release permits from previous queues
    until the query finishes execution.

  * The last rule is a catch all, which puts all queries into the user's personal queue.

All together these rules implement the policy that ``bob`` is an admin and
all other users are subject to the follow limits:

  * Users are allowed to have up to 5 queries running.

  * No more than 10 ``pipeline`` queries may run at once.

  * No more than 100 other queries may run at once.

.. code-block:: json

    {
      "queues": {
        "user.${USER}": {
          "maxConcurrent": 5,
          "maxQueued": 20
        },
        "pipeline": {
          "maxConcurrent": 10,
          "maxQueued": 100
        },
        "admin": {
          "maxConcurrent": 100,
          "maxQueued": 100
        },
        "global": {
          "maxConcurrent": 100,
          "maxQueued": 1000
        }
      },
      "rules": [
        {
          "user": "bob",
          "queues": ["admin"]
        },
        {
          "source": ".*pipeline.*",
          "queues": [
            "user.${USER}",
            "pipeline",
            "global"
          ]
        },
        {
          "queues": [
            "user.${USER}",
            "global"
          ]
        }
      ]
    }
