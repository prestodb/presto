--q01 query to test pass through mechanism
ExecuteWxdQueryOptimizer 'values prestosql ( 'select
                         	l_orderkey
                         from
                         	lineitem
                         limit 100;', '<catalog>', '<schema>');'
