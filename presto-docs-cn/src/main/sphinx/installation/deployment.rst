================
部署Presto
================

安装Presto
-----------------

下载Presto服务端打包程序并解压，:download:`server`.
压缩包将包含一个最上层的目录
|presto_server_release|，我们将这个目录作为安装目录。

Presto需要一个 *data* 目录用来存储日志(log)和元数据(metadata)等信息。
为了更加轻松的升级Presto，我们推荐建立一个在安装目录之外建立一个data目录。

配置Presto
------------------

在安装目录下建立一个 ``etc`` 目录。
这个目录将用来保存如下配置信息：

* node配置：指定每个节点环境变量的配置信息
* JVM配置：Java虚拟机的命令行选项
* Presto配置：Presto服务的配置信息
* Catalog配置：连接器的配置信息(数据源)

.. _presto_node_properties:

node配置
^^^^^^^^^^^^^^^

node配置文件 ``etc/node.properties`` 包含了每个节点的配置信息。
一个 *node* 是一个独立运行在一台机器上的Presto实例。当部署的系统第一次运行时会自动创建一个。
下面是最小的 ``etc/node.properties``：

.. code-block:: none

    node.environment=production
    node.id=ffffffff-ffff-ffff-ffff-ffffffffffff
    node.data-dir=/var/presto/data

配置文件各个字段的意义如下：

* ``node.environment``:
  环境的名字，在同一个集群的Presto节点必须拥有相同的名字。

* ``node.id``:
  Presto节点的唯一标识符，不同节点的标识符必须不同。升级和重启标识符必须保证不变。
  如果一台机器运行多个Presto实例(也就是一个节点多个node)，不同节点的标识符也必须不同。

* ``node.data-dir``:
  data目录的位置(文件系统路径)。Presto的log和数据文件会保存在这个路径下。

.. _presto_jvm_config:

JVM配置
^^^^^^^^^^

JVM配置文件，``etc/jvm.config`` 用来执行Java虚拟机的选项会以列表的形式保存在这个文件中。
配置文件每行包含一个JVM选项。这些选项不会被shell调用执行，所以选项是可以直接包含空格以及其他的特殊字符，不需要被引用或者转义(下面的 ``OnOutOfMemoryError`` 选项是个很好的例子)。

下面是让我们了解 ``etc/jvm.config`` 内容的一个很好的例子：

.. code-block:: none

    -server
    -Xmx16G
    -XX:+UseConcMarkSweepGC
    -XX:+ExplicitGCInvokesConcurrent
    -XX:+CMSClassUnloadingEnabled
    -XX:+AggressiveOpts
    -XX:+HeapDumpOnOutOfMemoryError
    -XX:OnOutOfMemoryError=kill -9 %p
    -XX:PermSize=150M
    -XX:MaxPermSize=150M
    -XX:ReservedCodeCacheSize=150M
    -Xbootclasspath/p:/var/presto/installation/lib/floatingdecimal-0.1.jar

Because an ``OutOfMemoryError`` will typically leave the JVM in an
inconsistent state, we write a heap dump (for debugging) and forcibly
terminate the process when this occurs.

Presto compiles queries to bytecode at runtime and thus produces many classes,
so we increase the permanent generation size (the garbage collector region
where classes are stored) and enable class unloading.

The last option in the above configuration loads the
`floatingdecimal <https://github.com/airlift/floatingdecimal>`_
patch for the JDK that substantially improves performance when parsing
floating point numbers. This is important because many Hive file formats
store floating point values as text. Change the path
``/var/presto/installation`` to match the Presto installation directory.

.. note::

    When using Java 8, remove the ``-XX:PermSize``, ``-XX:MaxPermSize`` and
    ``-Xbootclasspath`` options. The ``PermSize`` options are not supported
    and the floatingdecimal patch is only for Java 7.

.. _config_properties:

Presto配置
^^^^^^^^^^^^^^^^^

The config properties file, ``etc/config.properties``, contains the
configuration for the Presto server. Every Presto server can function
as both a coordinator and a worker, but dedicating a single machine
to only perform coordination work provides the best performance on
larger clusters.

The following is a minimal configuration for the coordinator:

.. code-block:: none

    coordinator=true
    node-scheduler.include-coordinator=false
    http-server.http.port=8080
    task.max-memory=1GB
    discovery-server.enabled=true
    discovery.uri=http://example.net:8080

And this is a minimal configuration for the workers:

.. code-block:: none

    coordinator=false
    http-server.http.port=8080
    task.max-memory=1GB
    discovery.uri=http://example.net:8080

Alternatively, if you are setting up a single machine for testing that
will function as both a coordinator and worker, use this configuration:

.. code-block:: none

    coordinator=true
    node-scheduler.include-coordinator=true
    http-server.http.port=8080
    task.max-memory=1GB
    discovery-server.enabled=true
    discovery.uri=http://example.net:8080

These properties require some explanation:

* ``coordinator``:
  Allow this Presto instance to function as a coordinator
  (accept queries from clients and manage query execution).

* ``node-scheduler.include-coordinator``:
  Allow scheduling work on the coordinator.
  For larger clusters, processing work on the coordinator
  can impact query performance because the machine's resources are not
  available for the critical task of scheduling, managing and monitoring
  query execution.

* ``http-server.http.port``:
  Specifies the port for the HTTP server. Presto uses HTTP for all
  communication, internal and external.

* ``task.max-memory=1GB``:
  The maximum amount of memory used by a single task
  (a fragment of a query plan running on a specific node).
  In particular, this limits the number of groups in a ``GROUP BY``,
  the size of the right-hand table in a ``JOIN``, the number of rows
  in an ``ORDER BY`` or the number of rows processed by a window function.
  This value should be tuned based on the number of concurrent queries and
  the size and complexity of queries.  Setting it too low will limit the
  queries that can be run, while setting it too high will cause the JVM
  to run out of memory.

* ``discovery-server.enabled``:
  Presto uses the Discovery service to find all the nodes in the cluster.
  Every Presto instance will register itself with the Discovery service
  on startup. In order to simplify deployment and avoid running an additional
  service, the Presto coordinator can run an embedded version of the
  Discovery service. It shares the HTTP server with Presto and thus uses
  the same port.

* ``discovery.uri``:
  The URI to the Discovery server. Because we have enabled the embedded
  version of Discovery in the Presto coordinator, this should be the
  URI of the Presto coordinator. Replace ``example.net:8080`` to match
  the host and port of the Presto coordinator. This URI must not end
  in a slash.

日志等级
^^^^^^^^^^

The optional log levels file, ``etc/log.properties``, allows setting the
minimum log level for named logger hierarchies. Every logger has a name,
which is typically the fully qualified name of the class that uses the logger.
Loggers have a hierarchy based on the dots in the name (like Java packages).
For example, consider the following log levels file:

.. code-block:: none

    com.facebook.presto=INFO

This would set the minimum level to ``INFO`` for both
``com.facebook.presto.server`` and ``com.facebook.presto.hive``.
The default minimum level is ``INFO``
(thus the above example does not actually change anything).
There are four levels: ``DEBUG``, ``INFO``, ``WARN`` and ``ERROR``.

Catalog配置
^^^^^^^^^^^^^^^^^^

Presto accesses data via *connectors*, which are mounted in catalogs.
The connector provides all of the schemas and tables inside of the catalog.
For example, the Hive connector maps each Hive database to a schema,
so if the Hive connector is mounted as the ``hive`` catalog, and Hive
contains a table ``bar`` in database ``foo``, that table would be accessed
in Presto as ``hive.foo.bar``.

Catalogs are registered by creating a catalog properties file
in the ``etc/catalog`` directory.
For example, create ``etc/catalog/jmx.properties`` with the following
contents to mount the ``jmx`` connector as the ``jmx`` catalog:

.. code-block:: none

    connector.name=jmx

Hive
""""

Presto includes Hive connectors for multiple versions of Hadoop:

* ``hive-hadoop1``: Apache Hadoop 1.x
* ``hive-hadoop2``: Apache Hadoop 2.x
* ``hive-cdh4``: Cloudera CDH 4
* ``hive-cdh5``: Cloudera CDH 5

Create ``etc/catalog/hive.properties`` with the following contents
to mount the ``hive-cdh4`` connector as the ``hive`` catalog,
replacing ``hive-cdh4`` with the proper connector for your version
of Hadoop and ``example.net:9083`` with the correct host and port
for your Hive metastore Thrift service:

.. code-block:: none

    connector.name=hive-cdh4
    hive.metastore.uri=thrift://example.net:9083

If your Hive metastore references files stored on a federated HDFS,
or if your HDFS cluster requires other non-standard client options
to access it, add this property to reference your HDFS config files:

.. code-block:: none

    hive.config.resources=/etc/hadoop/conf/core-site.xml,/etc/hadoop/conf/hdfs-site.xml

Note that Presto configures the HDFS client automatically for most
setups and does not require any configuration files. Only specify
additional configuration files if absolutely necessary. We also
recommend minimizing the configuration files to have the minimum set
of requried properties, as additional properties may cause problems.

You can have as many catalogs as you need, so if you have additional
Hive clusters, simply add another properties file to ``etc/catalog``
with a different name (making sure it ends in ``.properties``).

Cassandra
"""""""""

Create ``etc/catalog/cassandra.properties`` with the following contents
to mount the ``cassandra`` connector as the ``cassandra`` catalog,
replacing ``host1,host2`` with a comma-separated list of the Cassandra
nodes used to discovery the cluster topology:

.. code-block:: none

    connector.name=cassandra
    cassandra.contact-points=host1,host2

You will also need to set ``cassandra.native-protocol-port`` if your
Cassandra nodes are not using the default port (9142).

.. _running_presto:

启动Presto
--------------

The installation directory contains the launcher script in ``bin/launcher``.
Presto can be started as a daemon by running running the following:

.. code-block:: none

    bin/launcher start

Alternatively, it can be run in the foreground, with the logs and other
output being written to stdout/stderr (both streams should be captured
if using a supervision system like daemontools):

.. code-block:: none

    bin/launcher run

Run the launcher with ``--help`` to see the supported commands and
command line options. In particular, the ``--verbose`` option is
very useful for debugging the installation.

After launching, you can find the log files in ``var/log``:

* ``launcher.log``:
  This log is created by the launcher and is connected to the stdout
  and stderr streams of the server. It will contain a few log messages
  that occur while the server logging is being initialized and any
  errors or diagnostics produced by the JVM.

* ``server.log``:
  This is the main log file used by Presto. It will typically contain
  the relevant information if the server fails during initialization.
  It is automatically rotated and compressed.

* ``http-request.log``:
  This is the HTTP request log which contains every HTTP request
  received by the server. It is automatically rotated and compressed.
