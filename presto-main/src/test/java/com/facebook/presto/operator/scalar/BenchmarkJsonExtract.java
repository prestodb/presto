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
public class BenchmarkJsonExtract
{
    @Benchmark
    public void benchmarkJsonExtract(Blackhole blackhole, JsonBenchmarksData data)
    {
        blackhole.consume(JsonFunctions.jsonExtract(data.getInputJson(), data.getJsonPath()));
    }

    @Benchmark
    public void benchmarkJsonExtractJayway(Blackhole blackhole, JsonBenchmarksData data)
            throws IOException
    {
        blackhole.consume(JsonFunctions.jsonExtractJayway(data.getInputJson(), data.getJsonPathAsSlice()));
    }

    @State(Thread)
    public static class JsonBenchmarksData
    {
        private static final String JSON = "{\n" +
                "  \"store\": {\n" +
                "    \"book\": [\n" +
                "      {\n" +
                "        \"category\": \"reference\",\n" +
                "        \"author\": \"Nigel Rees\",\n" +
                "        \"title\": \"Sayings of the Century\",\n" +
                "        \"price\": 8.95\n" +
                "      },\n" +
                "      {\n" +
                "        \"category\": \"fiction\",\n" +
                "        \"author\": \"Evelyn Waugh\",\n" +
                "        \"title\": \"Sword of Honour\",\n" +
                "        \"price\": 12.99\n" +
                "      },\n" +
                "      {\n" +
                "        \"category\": \"fiction\",\n" +
                "        \"author\": \"Herman Melville\",\n" +
                "        \"title\": \"Moby Dick\",\n" +
                "        \"isbn\": \"0-553-21311-3\",\n" +
                "        \"price\": 8.99\n" +
                "      },\n" +
                "      {\n" +
                "        \"category\": \"fiction\",\n" +
                "        \"author\": \"J. R. R. Tolkien\",\n" +
                "        \"title\": \"The Lord of the Rings\",\n" +
                "        \"isbn\": \"0-395-19395-8\",\n" +
                "        \"price\": 22.99\n" +
                "      }\n" +
                "    ],\n" +
                "    \"statuses\": [\n" +
                "      {\n" +
                "        \"metadata\": {\n" +
                "          \"result_type\": \"recent\",\n" +
                "          \"iso_language_code\": \"ja\"\n" +
                "        },\n" +
                "        \"user\": {\n" +
                "          \"name\": \"AYUMI\",\n" +
                "          \"screen_name\": \"ayuu0123\",\n" +
                "          \"description\": \"元野球部マネージャー❤︎…最高の夏をありがとう…❤︎\"\n" +
                "        },\n" +
                "        \"entities\": {\n" +
                "          \"user_mentions\": [\n" +
                "            {\n" +
                "              \"screen_name\": \"aym0566x\",\n" +
                "              \"unicode_value\": \"前田あゆみ\",\n" +
                "              \"名前:前田あゆみ\": \"unicode key\"\n" +
                "            }\n" +
                "          ]\n" +
                "        }\n" +
                "      }\n" +
                "    ]\n" +
                "  }\n" +
                "}";

        @Param({
                "$.store.book.doesnt_exist",
                "$.store.book[3].author",
                "$.store.book",
                "$[\"store\"][\"book\"][0]",
                "$.store.statuses[0].entities.user_mentions[0].unicode_value",
                "$.store.statuses[0].entities.user_mentions[0].名前:前田あゆみ"
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
                .include(".*" + BenchmarkJsonExtract.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}
