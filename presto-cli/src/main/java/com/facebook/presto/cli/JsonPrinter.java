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
package com.facebook.presto.cli;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.io.Writer;
import java.util.List;

import static com.facebook.presto.cli.AlignedTablePrinter.formatHexDump;
import static java.util.Objects.requireNonNull;

public class JsonPrinter
        implements OutputPrinter
{
    private final List<String> fieldNames;
    private final Writer writer;

    public JsonPrinter(List<String> fieldNames, Writer writer)
    {
        this.fieldNames = ImmutableList.copyOf(requireNonNull(fieldNames, "fieldNames is null"));
        this.writer = requireNonNull(writer, "writer is null");
    }

    @Override
    public void printRows(List<List<?>> rows, boolean complete)
            throws IOException
    {
        JsonFactory jsonFactory = new JsonFactory();
        for (List<?> row : rows) {
            JsonGenerator jsonGenerator = jsonFactory.createGenerator(writer);
            jsonGenerator.writeStartObject();
            for (int position = 0; position < row.size(); position++) {
                String columnName = fieldNames.get(position);
                jsonGenerator.writeObjectField(columnName, formatValue(row.get(position)));
            }
            jsonGenerator.writeEndObject();
            jsonGenerator.writeRaw('\n');
            jsonGenerator.flush();
        }
    }

    @Override
    public void finish()
            throws IOException
    {
        writer.flush();
    }

    private static Object formatValue(Object o)
    {
        if (o instanceof byte[]) {
            return formatHexDump((byte[]) o);
        }
        return o;
    }
}
