======================
命令行接口
======================

Presto命令行接口提供一种基于终端的可交互查询方式。命令行接口是一个像Unix程序一样的
`可执行的jar包 <http://skife.org/java/unix/2011/06/20/really_executable_jars.html>`_
。

下载 :download:`cli`，重命名为 ``presto``，赋予可执行权限 ``chmod +x``，然后执行：

.. code-block:: none

    ./presto --server localhost:8080 --catalog hive --schema default

通过执行 ``--help`` 选项查看其他可用选项。

默认情况下，查询的结果是使用了 ``less`` 程序作为分页程序。
这种行为可以通过设置环境变量 ``PRESTO_PAGER`` 替换为其他程序，例如 ``more``，
或将其设置为空值的完全禁用分页。

