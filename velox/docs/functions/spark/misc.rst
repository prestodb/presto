====================================
Miscellaneous Functions
====================================

.. spark:function:: spark_partition_id() -> integer

    Returns the current partition id.
    The framework provides partition id through the configuration 'spark.partition_id'.
    It ensures deterministic data partitioning and assigns a unique partition id to each task in a deterministic way.
    Consequently, this function is marked as deterministic, enabling Velox to perform constant folding on it.
