===================
Queue Configuration
===================

The queueing rules are defined in a JSON file and control the number of queries
that can be submitted to Presto and the quota of running queries per queue.
The filename of the JSON config file should be specified in ``query.queue-config-file``
config property.

Rules that specify multiple queues will cause the query to enter the queues sequentially.
Rules are processed sequentially and the first one that matches will be used.
In the example configuration below, there are five queue templates. In the
``user.${USER}`` queue, ``${USER}`` will be expanded to the name of the user
that submitted the query. ``${SOURCE}`` is also supported, which expands to the
source submitting the query.

There are also five rules that define which queries go into which queues:

  * The first rule makes ``bob`` an admin.

  * The second rule states that all queries that come from a source that includes ``pipeline``
    should first be queued in the user's personal queue, then the ``pipeline`` queue. When a
    query enters a new queue, it doesn't leave previous queues until the query finishes execution.

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
