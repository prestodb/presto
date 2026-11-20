======================================
Logical Dataset Rewrite (Experimental)
======================================

A *logical dataset* is a named, abstract view of data that may be backed
by one or more physical artifacts. The backing artifacts are semantically
equivalent but may differ in layout, granularity, or storage
characteristics. The choice of which physical artifact to use for a
given query is left entirely to the implementation of the logical dataset.

When this rewrite is enabled, Presto intercepts references to logical
dataset names before analysis and rewrites them to the appropriate
physical artifact. The substitution is transparent to the caller.

Enable with::

    SET SESSION experimental_logical_dataset_rewrite_enabled = true;
