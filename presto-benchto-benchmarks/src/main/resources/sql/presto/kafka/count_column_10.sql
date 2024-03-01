select
   count(c_bigint_1), count(c_double_1), count(c_varchar_1),
   count(c_bigint_2), count(c_double_2), count(c_varchar_2),
   count(c_bigint_3), count(c_double_3), count(c_varchar_3),
   count(c_bigint_4), count(c_double_4), count(c_varchar_4),
   count(c_bigint_5), count(c_double_5), count(c_varchar_5),
   count(c_bigint_6), count(c_double_6), count(c_varchar_6),
   count(c_bigint_7), count(c_double_7), count(c_varchar_7),
   count(c_bigint_8), count(c_double_8), count(c_varchar_8),
   count(c_bigint_9), count(c_double_9), count(c_varchar_9),
   count(c_bigint_10), count(c_double_10), count(c_varchar_10)
from ${database}.${schema}.${table}
