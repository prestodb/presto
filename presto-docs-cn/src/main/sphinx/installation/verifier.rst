.. _presto_verifier:

===============
Presto校验器
===============

Presto校验器用来测试Presto是否和其他数据库有冲突(例如Mysql)，或者测试两个Presto集群之间是否有冲突。
我们在开发的过程中用它持续集成，以确保兼容以前的版本。

创建一个Mysql数据库，并通过下面的代码建立一个verifier_queries表：

.. code-block:: sql

    CREATE TABLE verifier_queries(
        id INT NOT NULL AUTO_INCREMENT,
        suite VARCHAR(256) NOT NULL,
        name VARCHAR(256),
        test_catalog VARCHAR(256) NOT NULL,
        test_schema VARCHAR(256) NOT NULL,
        test_query TEXT NOT NULL,
        control_catalog VARCHAR(256) NOT NULL,
        control_schema VARCHAR(256) NOT NULL,
        control_query TEXT NOT NULL,
        PRIMARY KEY (id)
    );

给校验器创建一个配置文件如下：

.. code-block:: none

    suite=my_suite
    query-database=jdbc:mysql://localhost:3306/my_database?user=my_username&password=my_password
    control.gateway=jdbc:presto://localhost:8080
    test.gateway=jdbc:presto://localhost:8081
    thread-count=1

最后下载 :download:`verifier`，重命名为 ``verifier``，赋予可执行权限 ``chmod +x``，然后运行：

.. code-block:: none

    ./verifier config.properties
