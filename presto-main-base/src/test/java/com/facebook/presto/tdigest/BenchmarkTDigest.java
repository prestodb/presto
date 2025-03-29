/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Licensed to Ted Dunning under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.facebook.presto.tdigest;

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

import static com.facebook.presto.tdigest.TDigest.createTDigest;

@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(3)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
public class BenchmarkTDigest
{
    private static final int NUMBER_OF_ENTRIES = 1_000_000;
    private static final int STANDARD_COMPRESSION_FACTOR = 100;

    @State(Scope.Thread)
    public static class Data
    {
        private long[] normalDistribution1;
        private long[] normalDistribution2;

        @Setup
        public void setup()
        {
            normalDistribution1 = makeNormalValues(NUMBER_OF_ENTRIES);
            normalDistribution2 = makeNormalValues(NUMBER_OF_ENTRIES);
        }

        private long[] makeNormalValues(int size)
        {
            long[] values = new long[size];
            for (int i = 0; i < size; i++) {
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
        protected Slice serializedDigest1;

        @Setup
        public void setup(Data data)
        {
            digest1 = makeTDigest(data.normalDistribution1);
            digest2 = makeTDigest(data.normalDistribution2);
            serializedDigest1 = digest1.serialize();
        }

        private TDigest makeTDigest(long[] values)
        {
            TDigest result = createTDigest(STANDARD_COMPRESSION_FACTOR);
            int k = 1_000_000 / NUMBER_OF_ENTRIES;
            for (int i = 0; i < k; i++) {
                for (long value : values) {
                    result.add(value);
                }
            }
            return result;
        }
    }

    @State(Scope.Thread)
    public static class DigestWithQuantile
            extends Digest
    {
        // allows testing how fast different quantiles can be computed
        @Param({"0.0001", "0.01", "0.2500", "0.5000", "0.7500", "0.9999"})
        float quantile;
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OperationsPerInvocation(NUMBER_OF_ENTRIES)
    public TDigest benchmarkInsertsT(Data data)
    {
        TDigest digest = createTDigest(STANDARD_COMPRESSION_FACTOR);
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
    public double benchmarkQuantilesT(DigestWithQuantile data)
    {
        return data.digest1.getQuantile(data.quantile);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public TDigest benchmarkCopyT(Digest data)
    {
        TDigest copy = createTDigest(data.digest1.getCompressionFactor());
        copy.merge(data.digest1);
        return copy;
    }

    @Benchmark @BenchmarkMode(Mode.AverageTime)
    public TDigest benchmarkMergeT(Digest data)
    {
        TDigest merged = createTDigest(data.digest1.serialize());
        merged.merge(data.digest2);
        return merged;
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public TDigest benchmarkDeserializeT(Digest data)
    {
        return createTDigest(data.serializedDigest1);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public Slice benchmarkSerializeT(Digest data)
    {
        return data.digest1.serialize();
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkTDigest.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}
