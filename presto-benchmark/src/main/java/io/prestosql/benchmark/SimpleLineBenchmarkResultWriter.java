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

import com.google.common.base.Joiner;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class SimpleLineBenchmarkResultWriter
        implements BenchmarkResultHook
{
    private final Writer writer;

    public SimpleLineBenchmarkResultWriter(OutputStream outputStream)
    {
        writer = new OutputStreamWriter(requireNonNull(outputStream, "outputStream is null"));
    }

    @Override
    public BenchmarkResultHook addResults(Map<String, Long> results)
    {
        requireNonNull(results, "results is null");
        try {
            Joiner.on(",").withKeyValueSeparator(":").appendTo(writer, results);
            writer.write('\n');
            writer.flush();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return this;
    }

    @Override
    public void finished()
    {
        // No-op
    }
}
