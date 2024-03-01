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
package com.facebook.presto.benchmark;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class JsonBenchmarkResultWriter
        implements BenchmarkResultHook
{
    private final JsonGenerator jsonGenerator;

    public JsonBenchmarkResultWriter(OutputStream outputStream)
    {
        requireNonNull(outputStream, "outputStream is null");
        try {
            jsonGenerator = new JsonFactory().createGenerator(outputStream, JsonEncoding.UTF8);
            jsonGenerator.writeStartObject();
            jsonGenerator.writeArrayFieldStart("samples");
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public BenchmarkResultHook addResults(Map<String, Long> results)
    {
        requireNonNull(results, "results is null");
        try {
            jsonGenerator.writeStartObject();
            for (Map.Entry<String, Long> entry : results.entrySet()) {
                jsonGenerator.writeNumberField(entry.getKey(), entry.getValue());
            }
            jsonGenerator.writeEndObject();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return this;
    }

    @Override
    public void finished()
    {
        try {
            jsonGenerator.writeEndArray();
            jsonGenerator.writeEndObject();
            jsonGenerator.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
