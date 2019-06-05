package com.facebook.presto.operator.scalar;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(2)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
public class BenchmarkTDigest
{
    private static final int NUMBER_OF_ENTRIES = 1_000_000;
    private static final int STANDARD_COMPRESSION_FACTOR = 100;
    private static final double STANDARD_MAX_ERROR = 0.01;

    @State(Scope.Thread)
    public static class Data
    {
        private long[] normalDistribution1;
        private long[] normalDistribution2;
        private long[] randomDistribution1;
        private long[] randomDistribution2;

        @Setup
        public void setup()
        {
            normalDistribution1 = makeNormalValues(NUMBER_OF_ENTRIES);
            normalDistribution2 = makeNormalValues(NUMBER_OF_ENTRIES);
            randomDistribution1 = makeRandomValues(NUMBER_OF_ENTRIES);
            randomDistribution2 = makeRandomValues(NUMBER_OF_ENTRIES);
        }

        private long[] makeNormalValues(int size)
        {
            long[] values = new long[size];
            for (int i = 0; i < size; i++) {
                // generate values from a large domain but not many distinct values
                long value = Math.abs((long) (ThreadLocalRandom.current().nextGaussian() * 1_000_000_000));
                values[i] = value;
            }

            return values;
        }

        private long[] makeRandomValues(int size)
        {
            long[] values = new long[size];
            for (int i = 0; i < size; i++) {
                values[i] = (long) (Math.random() * 1_000_000_000);
            }

            return values;
        }
    }

    @State(Scope.Thread)
    public static class Digest
    {
        protected TDigest digest1;
        protected TDigest digest2;
        protected QuantileDigest digest3;
        protected QuantileDigest digest4;
        protected Slice serializedDigest1;
        protected Slice serializedDigest3;

        @Setup
        public void setup(Data data)
        {
            digest1 = makeTDigest(data.normalDistribution1);
            digest2 = makeTDigest(data.randomDistribution1);
            digest3 = makeQDigest(data.normalDistribution1);
            digest4 = makeQDigest(data.randomDistribution1);
            serializedDigest1 = digest1.serialize();
            serializedDigest3 = digest3.serialize();
        }

        private TDigest makeTDigest(long[] values)
        {
            TDigest result = TDigest.createMergingDigest(STANDARD_COMPRESSION_FACTOR);
            int k = 1_000_000 / NUMBER_OF_ENTRIES;
            for (int i = 0; i < k; i++) {
                for (long value : values) {
                    result.add(value);
                }
            }
            return result;
        }

        private QuantileDigest makeQDigest(long[] values)
        {
            QuantileDigest result = new QuantileDigest(STANDARD_MAX_ERROR);
            for (long value : values) {
                result.add(value);
            }
            return result;
        }
    }

    @State(Scope.Thread)
    public static class DigestWithQuantile
            extends Digest
    {
        @Param({"0.0001", "0.01", "0.2500", "0.5000", "0.7500", "0.9999"})
        float quantile;
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OperationsPerInvocation(NUMBER_OF_ENTRIES)
    public TDigest benchmarkInsertsT(Data data)
    {
        TDigest digest = TDigest.createMergingDigest(STANDARD_COMPRESSION_FACTOR);
        int k = 1_000_000 / NUMBER_OF_ENTRIES;
        for (int i = 0; i < k; i++) {
            for (long value : data.normalDistribution1) {
                digest.add(value);
            }
        }

        return digest;
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OperationsPerInvocation(NUMBER_OF_ENTRIES)
    public QuantileDigest benchmarkInsertsQ(Data data)
    {
        QuantileDigest digest = new QuantileDigest(STANDARD_MAX_ERROR);

        for (int i = 0; i < 10; i++) {
            for (long value : data.normalDistribution1) {
                digest.add(value);
            }
        }
        return digest;
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public double benchmarkQuantilesT(DigestWithQuantile data)
    {
        return data.digest1.quantile(data.quantile);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public long benchmarkQuantilesQ(DigestWithQuantile data)
    {
        return data.digest3.getQuantile(data.quantile);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public TDigest benchmarkCopyT(Digest data)
    {
        TDigest copy = TDigest.createMergingDigest(data.digest1.compression());
        copy.add(ImmutableList.of(data.digest1));
        return copy;
        //alternatively
        //return MergingDigest.deserialize(data.digest1.serialize());
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public QuantileDigest benchmarkCopyQ(Digest data)
    {
        return new QuantileDigest(data.digest3);
    }

    @Benchmark @BenchmarkMode(Mode.AverageTime)
    public TDigest benchmarkMergeT(Digest data)
    {
        TDigest merged = MergingDigest.deserialize(data.digest1.serialize());
        merged.add(data.digest2);
        return merged;
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public QuantileDigest benchmarkMergeQ(Digest data)
    {
        QuantileDigest merged = new QuantileDigest(data.digest3);
        merged.merge(data.digest4);
        return merged;
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public TDigest benchmarkDeserializeT(Digest data)
    {
        return MergingDigest.deserialize(data.serializedDigest1);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public QuantileDigest benchmarkDeserializeQ(Digest data)
    {
        return new QuantileDigest(data.serializedDigest3);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public Slice benchmarkSerializeT(Digest data)
    {
        return data.digest1.serialize();
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public Slice benchmarkSerializeQ(Digest data)
    {
        return data.digest3.serialize();
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkTDigest.class.getSimpleName() + ".*")
                //.addProfiler(GCProfiler.class)
                .build();

        new Runner(options).run();
    }
}
