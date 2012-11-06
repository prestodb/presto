package com.facebook.presto.benchmark;

public class PredicateFilterSqlBenchmark
        extends AbstractSqlBenchmark
{
    public PredicateFilterSqlBenchmark()
    {
        super("sql_predicate_filter", 5, 50, "select totalprice from orders where totalprice > 50000");
    }

    public static void main(String[] args)
    {
        new PredicateFilterSqlBenchmark().runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
