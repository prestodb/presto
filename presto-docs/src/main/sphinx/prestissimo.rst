***************************
Prestissimo Developer Guide
***************************

This guide is intended for Prestissimo contributors and plugin developers.

Note: Prestissimo is in active development. 

Prestissimo is a C++ drop-in replacement for Presto workers based on the Velox
library. It implements the same RESTful endpoints as Java workers using the
Proxygen C++ HTTP framework. Since communication with the Java coordinator and
across workers is only done using the REST endpoints, Prestissimo does not use
JNI and it is commonly deployed in such a way as to avoid having a JVM process
on worker nodes.

Prestissimo's codebase is located at `presto-native-execution
<https://github.com/prestodb/presto/tree/master/presto-native-execution>`_.


.. toctree::
    :maxdepth: 1

    prestissimo/prestissimo-features
    prestissimo/prestissimo-limitations
