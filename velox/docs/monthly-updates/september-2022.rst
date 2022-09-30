*********************
September 2022 Update
*********************

Documentation
=============

* Document :ref:`flat-no-nulls fast path <expr-flat-no-nulls>` in expression evaluation.
* Document :doc:`encoding-preserving vector serialization format <../develop/debugging/vector-saver>`.
* Document :doc:`ANTI join semantics <../develop/anti-join>`.
* Document :doc:`RIGHT SEMI join <../develop/joins>`.
* Document :doc:`Velox types <../develop/types>`.

Core Library
============

* Add spilling support for hash join build.
* Add support for :ref:`flattening concat-like function calls<expr-flatten-concat>`
  in expression evaluation.

Presto Functions
================

* Add :func:`rank`, :func:`dense_rank` and :func:`percent_rank` window functions.
* Add :func:`zip_with` function.
* Add support for casting JSON string to a struct.
* Add support for DECIMAL inputs to :func:`multiply`, :func:`divide`,
  :func:`between`, :func:`eq`, :func:`gt`, :func:`lt`, :func:`gte` and
  :func:`lte` functions.
* Add support for TIMESTAMP WITH TIME ZONE inputs to :func:`date_format` function.
* Add support for calculating multiple percentiles to :func:`approx_percentile` function. :pr:`2418`
* Optimize LIKE operator for prefix, suffix and other simple patterns. :pr:`1763`
* Optimize :func:`concat` function. :pr:`2584`, :pr:`2551`, :pr:`2498`
* Fix :func:`approx_percentile` function when used in intermediate aggregation.
* Fix :func:`min` and :func:`max` aggregate functions for DATE inputs.

Substrait Extension
===================

* Add support for IF and SWITCH expressions.

Performance and Correctness
===========================

* Add support for AND/OR expressions and functions with variadic arguments to Fuzzer.
* Add logic to save input vector and expression SQL to files when expression
  evaluation fails. This functionality is disabled by default.
  Use `--velox_save_input_on_expression_any_failure_path=/tmp` gflag to enable
  for all exception
  or `--velox_save_input_on_expression_system_failure_path=/tmp` to enable for
  system exceptions only. :pr:`2662`
* Fix multiple issues found by the `ThreadSanitizer <https://clang.llvm.org/docs/ThreadSanitizer.html>`_.

Credits
=======

Aditi Pandit, Austin Dickey, Behnam Robatmili, Bikramjeet Vig, Bo Yang, Deepak
Majeti, Huameng Jiang, Jake Jung, Jialiang Tan, Jimmy Lu, Karteek Murthy Samba
Murthy, Kevin Wilfong, Krishna Pai, Laith Sakka, Masha Basmanova, Michael
Bolin, Miłosz Linkiewicz, Orri Erling, Pavel Solodovnikov, Pedro Eugenio Rocha
Pedreira, Pramod, Randeep Singh, Raúl Cumplido, Serge Druzkin,
Sergey Pershin, Wei He, Xiaoxuan Meng, Xuedong Luan, Zeyi (Rice) Fan, Zhang,
Chaojun, Zhenyuan Zhao, erdembilegt.j, leoluan2009, lingbin, tanjialiang,
xiaoxmeng, yingsu00, zhejiangxiaomai, zhaozhenhui
