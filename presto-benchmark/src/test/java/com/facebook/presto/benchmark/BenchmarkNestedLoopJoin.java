package com.facebook.presto.benchmark;

import com.facebook.presto.spi.Page;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.List;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.openjdk.jmh.annotations.Scope.Thread;

@SuppressWarnings("MethodMayBeStatic")
@State(Thread)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
@Fork(2)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
public class BenchmarkNestedLoopJoin
{
    @State(Thread)
    public static class Context
    {
        private MemoryLocalQueryRunner queryRunner = new MemoryLocalQueryRunner();

        @Param({"count(*)", "sum(l.partkey) + sum(o.orderkey)"})
        private String project;

        public MemoryLocalQueryRunner getQueryRunner()
        {
            return queryRunner;
        }
    }

    @Benchmark
    public List<Page> benchmark(Context context)
    {
        return context.getQueryRunner().execute(
                format("select %s from tpch.tiny.lineitem l join tpch.tiny.orders o on (l.partkey + o.orderkey) %% 2 = 0", context.project));
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkNestedLoopJoin.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}
