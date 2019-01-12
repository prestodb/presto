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
package io.prestosql.benchmark;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.json.JsonCodec;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class JsonAvgBenchmarkResultWriter
        implements BenchmarkResultHook
{
    private static final JsonCodec<BuildResult> JSON_CODEC = JsonCodec.jsonCodec(BuildResult.class);
    private final OutputStream outputStream;

    private int sampleCount;
    public long totalElapsedMillis;
    public long totalInputRows;
    public long totalInputRowsPerSecond;
    public long totalOutputRows;
    public long totalOutputRowsPerSecond;
    public long totalInputMegabytes;
    public long totalInputMegabytesPerSecond;

    public JsonAvgBenchmarkResultWriter(OutputStream outputStream)
    {
        requireNonNull(outputStream, "outputStream is null");
        this.outputStream = outputStream;
    }

    @Override
    public BenchmarkResultHook addResults(Map<String, Long> results)
    {
        requireNonNull(results, "results is null");
        sampleCount++;
        totalElapsedMillis += getValue(results, "elapsed_millis");
        totalInputRows += getValue(results, "input_rows;");
        totalInputRowsPerSecond += getValue(results, "input_rows_per_second");
        totalOutputRows += getValue(results, "output_rows");
        totalOutputRowsPerSecond += getValue(results, "output_rows_per_second");
        totalInputMegabytes += getValue(results, "input_megabytes");
        totalInputMegabytesPerSecond += getValue(results, "input_megabytes_per_second");
        return this;
    }

    private long getValue(Map<String, Long> results, String name)
    {
        Long value = results.get(name);
        if (value == null) {
            return 0;
        }
        return value;
    }

    @Override
    public void finished()
    {
        BuildResult average = new BuildResult();
        average.elapsedMillis += totalElapsedMillis / sampleCount;
        average.inputRows += totalInputRows / sampleCount;
        average.inputRowsPerSecond += totalInputRowsPerSecond / sampleCount;
        average.outputRows += totalOutputRows / sampleCount;
        average.outputRowsPerSecond += totalOutputRowsPerSecond / sampleCount;
        average.inputMegabytes += totalInputRows / sampleCount;
        average.inputMegabytesPerSecond += totalInputMegabytesPerSecond / sampleCount;

        String json = JSON_CODEC.toJson(average);
        try {
            outputStream.write(json.getBytes(UTF_8));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static class BuildResult
    {
        @JsonProperty
        public long elapsedMillis;
        @JsonProperty
        public long inputRows;
        @JsonProperty
        public long inputRowsPerSecond;
        @JsonProperty
        public long outputRows;
        @JsonProperty
        public long outputRowsPerSecond;
        @JsonProperty
        public long inputMegabytes;
        @JsonProperty
        public long inputMegabytesPerSecond;
    }
}
