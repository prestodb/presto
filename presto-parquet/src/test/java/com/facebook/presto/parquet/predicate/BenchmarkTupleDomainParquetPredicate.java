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
package com.facebook.presto.parquet.predicate;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.parquet.DictionaryPage;
import com.facebook.presto.parquet.ParquetEncoding;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import org.apache.parquet.column.ColumnDescriptor;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;

@Fork(1)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkTupleDomainParquetPredicate
{
    @Benchmark
    public List<?> domainFromDictionary(Data data)
    {
        List<Domain> result = new ArrayList<>(data.bigintDictionaries.size());
        for (DictionaryDescriptor dictionary : data.bigintDictionaries) {
            result.add(TupleDomainParquetPredicate.getDomain(BIGINT, dictionary));
        }
        return result;
    }

    @State(Scope.Thread)
    public static class Data
    {
        public List<DictionaryDescriptor> bigintDictionaries;

        @Setup(Level.Iteration)
        public void init()
        {
            bigintDictionaries = new ArrayList<>();

            for (int i = 0; i < 1_000; i++) {
                bigintDictionaries.add(createBigintDictionary());
            }
        }

        private DictionaryDescriptor createBigintDictionary()
        {
            int size = 1_000;
            Slice slice;
            try (DynamicSliceOutput sliceOutput = new DynamicSliceOutput(0)) {
                for (int i = 0; i < size; i++) {
                    sliceOutput.appendLong(ThreadLocalRandom.current().nextLong());
                }
                slice = sliceOutput.slice();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }

            return new DictionaryDescriptor(
                    new ColumnDescriptor(new String[] {"path"}, INT64, 0, 0),
                    Optional.of(
                            new DictionaryPage(
                                    slice,
                                    slice.length(),
                                    size,
                                    ParquetEncoding.PLAIN)));
        }
    }

    @Test
    public void test()
    {
        Data data = new Data();
        data.init();

        domainFromDictionary(data);
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkTupleDomainParquetPredicate.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}
