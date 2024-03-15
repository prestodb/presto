Weighted Split Scheduling
===========================================

Weighted split scheduling is a feature introduced in Presto to enhance resource allocation and optimize query performance in distributed environments. This document provides an overview of weighted split scheduling and guides developers on how to leverage this feature effectively.
Understanding Weighted Split Scheduling

Code
----

The Example HTTP connector can be found in the ``presto-hive``
directory in the root of the Presto source tree.

-----------------------

Assigning Weights to Splits
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Developers can assign weights to splits using various metrics such as data size, complexity, or priority. Here's an example of assigning weights based on data size:

.. code-block:: java

    ConnectorSession session = ...;
    ConnectorSplit split = ...;
    long dataSize = split.getDataSizeInBytes();
    double weight = calculateWeight(dataSize);  // Custom method to calculate weight based on data size
    session.setProperty("split_weight", weight);

Scheduler Integration
~~~~~~~~~~~~~~~~~~~~~~

The weighted split scheduler integrates with Presto's existing scheduler to prioritize splits based on their assigned weights. Here's an example of integrating weighted split scheduling into the scheduler:

.. code-block:: java

    WeightedSplitScheduler scheduler = new WeightedSplitScheduler();
    scheduler.setWeightedSplitEnabled(true);

Resource Allocation
~~~~~~~~~~~~~~~~~~~~

Worker nodes receive splits from the scheduler based on their weights. Nodes with higher weights may receive more splits or additional resources to process them efficiently. Here's an example of resource allocation based on split weights:

.. code-block:: java

    double nodeCapacity = getNodeCapacity();  // Custom method to get node capacity
    double nodeWeight = getNodeWeight();      // Custom method to get node weight
    double splitWeight = getSplitWeight(split);  // Custom method to get split weight
    if (nodeWeight >= splitWeight) {
        allocateResources(split);  // Custom method to allocate resources to the split
    }

Enabling Weighted Split Scheduling
-----------------------------------

To enable weighted split scheduling, follow these steps:

1. Configuration: Adjust the configuration settings in the Presto cluster to enable weighted split scheduling. This may involve specifying parameters related to split weights, scheduler behavior, and resource allocation policies.

2. Query Planning: During query planning, developers can specify split weights or define rules to automatically assign weights based on predefined criteria.

3. Testing and Optimization: Before deploying weighted split scheduling in production environments, thoroughly test its impact on query performance and resource utilization. Fine-tune the weights and configuration settings as needed to optimize performance.

Best Practices
--------------

Consider the following best practices when using weighted split scheduling:

- Fine-tune Split Weights: Continuously monitor and adjust split weights based on evolving data characteristics and workload patterns.
- Monitor Performance: Regularly monitor query performance metrics to identify any bottlenecks or inefficiencies introduced by weighted split scheduling.
- Optimize Configuration: Experiment with different configuration settings to find the optimal balance between resource utilization and query performance.
