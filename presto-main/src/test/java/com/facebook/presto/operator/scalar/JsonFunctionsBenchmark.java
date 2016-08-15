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
package com.facebook.presto.operator.scalar;

import io.airlift.slice.Slice;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.io.IOException;

import static io.airlift.slice.Slices.utf8Slice;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.openjdk.jmh.annotations.Scope.Thread;

@State(Thread)
@OutputTimeUnit(NANOSECONDS)
@BenchmarkMode(AverageTime)
@Fork(1)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
public class JsonFunctionsBenchmark
{
    @Benchmark
    public void benchmarkJsonExtract(Blackhole blackhole, JsonBenchmarksData data)
    {
        blackhole.consume(JsonFunctions.jsonExtract(data.getInputJson(), data.getJsonPath()));
    }

    @Benchmark
    public void benchmarkJsonExtract_v2(Blackhole blackhole, JsonBenchmarksData data)
            throws IOException
    {
        blackhole.consume(JsonFunctions.jsonExtract_v2(data.getInputJson(), data.getJsonPathAsSlice()));
    }

    @State(Thread)
    public static class JsonBenchmarksData
    {
        private static final String JSON = "{\"store\": {\"book\": [{\"category\": \"reference\",\"author\": \"Nigel Rees\",\"title\": \"Sayings of the Century\",\"price\": 8.95}, " +
                "{\"category\": \"fiction\",\"author\": \"Evelyn Waugh\",\"title\": \"Sword of Honour\",\"price\": 12.99}," +
                " {\"category\": \"fiction\",\"author\": \"Herman Melville\",\"title\": \"Moby Dick\",\"isbn\": \"0-553-21311-3\",\"price\": 8.99}, " +
                "{\"category\": \"fiction\",\"author\": \"J. R. R. Tolkien\",\"title\": \"The Lord of the Rings\",\"isbn\": \"0-395-19395-8\",\"price\": 22.99}]," +
                "\"object\": {\"inner_object\": {\"array\": [{\"inner_array\": [{\"x\": \"y\"}]}]}}}}";

        @Param({"$.store.book.doesnt_exist",
                "$.store.book[3].author",
                "$.store.book",
                "$[\"store\"][\"book\"][0]",
                "$.store.object.inner_object.array[0].inner_array[0].x"
        })
        private String jsonPathString;

        public JsonPath getJsonPath()
        {
            return new JsonPath(jsonPathString);
        }

        public Slice getJsonPathAsSlice()
        {
            return utf8Slice(jsonPathString);
        }

        public Slice getInputJson()
        {
            return utf8Slice(JSON);
        }
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + JsonFunctionsBenchmark.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}
