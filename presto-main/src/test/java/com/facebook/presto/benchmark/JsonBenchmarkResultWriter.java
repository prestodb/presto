package com.facebook.presto.benchmark;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

public class JsonBenchmarkResultWriter
    implements BenchmarkResultHook
{
    private final JsonGenerator jsonGenerator;

    public JsonBenchmarkResultWriter(OutputStream outputStream)
    {
        Preconditions.checkNotNull(outputStream, "outputStream is null");
        try {
            jsonGenerator = new JsonFactory().createJsonGenerator(outputStream, JsonEncoding.UTF8);
            jsonGenerator.writeStartObject();
            jsonGenerator.writeArrayFieldStart("samples");
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public BenchmarkResultHook addResults(Map<String, Long> results)
    {
        Preconditions.checkNotNull(results, "results is null");
        try {
            jsonGenerator.writeStartObject();
            for (Map.Entry<String, Long> entry : results.entrySet()) {
                jsonGenerator.writeNumberField(entry.getKey(), entry.getValue());
            }
            jsonGenerator.writeEndObject();
        } catch (IOException e) {
            throw Throwables.propagate(e);
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
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }
}
