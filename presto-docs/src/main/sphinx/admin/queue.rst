===================
Queue Configuration
===================

The queueing rules are defined in a JSON file and control the number of queries
that can be submitted to Presto and the quota of running queries per queue.
Rules that specify multiple queues will cause the query to enter the queues sequentially.
Rules are processed sequentially and the first one that matches will be used.
In the example configuration below, there are five queue templates. In the
``user.${USER}`` queue, ``${USER}`` will be expanded to the name of the user
that submitted the query. ``${SOURCE}`` is also supported, which expands to the
source submitting the query.

There are also five rules that define which queries go into which queues:

* The first rule makes ``bob`` an admin.

* The second rule states that all queries submitted with the ``experimental_big_query``
  session property and that come from a source that includes ``pipeline`` should
  first be queued in the user's personal queue, then the ``pipeline`` queue, and
  finally the ``big`` queue.

* The third rule is the same as the previous, but without the ``experimental_big_query``
  requirement, and uses the ``global`` queue instead of the ``big`` queue.

* The last two rules are the same as the previous two, but without the
  match on ``pipeline`` in the source.

All together these rules implement the policy that ``bob`` is an admin and
all other users are subject to the follow limits:

  * Users are allowed to have up to 5 queries running.

  * ``big`` queries may only run one at a time.

  * No more than 10 ``pipeline`` queries may run at once.

  * No more than 100 non-``big`` queries may run at once.

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
        },
        "big": {
          "maxConcurrent": 1,
          "maxQueued": 10
        }
      },
      "rules": [
        {
          "user": "bob",
          "queues": ["admin"]
        },
        {
          "session.experimental_big_query": "true",
          "source": ".*pipeline.*",
          "queues": [
            "user.${USER}",
            "pipeline",
            "big"
          ]
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
          "session.experimental_big_query": "true",
          "queues": [
            "user.${USER}",
            "big"
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
