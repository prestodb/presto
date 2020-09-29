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

import com.facebook.presto.client.ClientTypeSignature;
import com.facebook.presto.client.ClientTypeSignatureParameter;
import com.facebook.presto.client.Column;
import com.facebook.presto.client.StatementClient;
import com.facebook.presto.common.type.NamedTypeSignature;
import com.facebook.presto.common.type.ParameterKind;
import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.facebook.presto.client.ClientTypeSignature.hasEnum;
import static com.facebook.presto.common.type.StandardTypes.ARRAY;
import static com.facebook.presto.common.type.StandardTypes.MAP;
import static com.facebook.presto.common.type.StandardTypes.ROW;
import static io.airlift.units.Duration.nanosSince;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public final class OutputHandler
        implements Closeable
{
    private static final Duration MAX_BUFFER_TIME = new Duration(3, SECONDS);
    private static final int MAX_BUFFERED_ROWS = 10_000;

    private final AtomicBoolean closed = new AtomicBoolean();
    private final List<List<?>> rowBuffer = new ArrayList<>(MAX_BUFFERED_ROWS);
    private final OutputPrinter printer;

    private long bufferStart;

    public OutputHandler(OutputPrinter printer)
    {
        this.printer = requireNonNull(printer, "printer is null");
    }

    public void processRow(List<?> row, Map<Integer, ColumnDataTransformer> transformers)
            throws IOException
    {
        if (rowBuffer.isEmpty()) {
            bufferStart = System.nanoTime();
        }

        rowBuffer.add(transformers.isEmpty() ? row : transformRow(row, transformers));
        if (rowBuffer.size() >= MAX_BUFFERED_ROWS) {
            flush(false);
        }
    }

    private List<?> transformRow(List<?> row, Map<Integer, ColumnDataTransformer> transformers)
    {
        ImmutableList.Builder<Object> builder = new ImmutableList.Builder<>();
        for (int i = 0; i < row.size(); i++) {
            if (!transformers.containsKey(i)) {
                builder.add(row.get(i));
                continue;
            }
            builder.add(transformers.get(i).transformValue(row.get(i)));
        }
        return builder.build();
    }

    @Override
    public void close()
            throws IOException
    {
        if (!closed.getAndSet(true)) {
            flush(true);
            printer.finish();
        }
    }

    public void processRows(StatementClient client)
            throws IOException
    {
        while (client.isRunning()) {
            Iterable<List<Object>> data = client.currentData().getData();
            if (data != null) {
                for (List<Object> row : data) {
                    processRow(unmodifiableList(row), getColumnTransformers(client.currentData().getColumns()));
                }
            }

            if (nanosSince(bufferStart).compareTo(MAX_BUFFER_TIME) >= 0) {
                flush(false);
            }

            client.advance();
        }
    }

    // Column transformers are used for pretty-printing of values in the CLI output.
    // Returns a map of column index to column transformers
    private Map<Integer, ColumnDataTransformer> getColumnTransformers(List<Column> columns)
    {
        Map<Integer, ColumnDataTransformer> transformers = new HashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            if (!hasEnum(column.getTypeSignature())) {
                continue;
            }
            transformers.put(i, new EnumLiteralTransformer(column.getTypeSignature()));
        }
        return transformers;
    }

    private void flush(boolean complete)
            throws IOException
    {
        if (!rowBuffer.isEmpty()) {
            printer.printRows(unmodifiableList(rowBuffer), complete);
            rowBuffer.clear();
        }
    }

    private interface ColumnDataTransformer
    {
        Object transformValue(Object value);
    }

    private static class EnumLiteralTransformer
            implements ColumnDataTransformer
    {
        private final ClientTypeSignature columnSignature;

        EnumLiteralTransformer(ClientTypeSignature columnSignature)
        {
            this.columnSignature = columnSignature;
        }

        @Override
        public Object transformValue(Object value)
        {
            return transformValue(columnSignature, value);
        }

        private static Object transformValue(ClientTypeSignature signature, Object value)
        {
            if (value == null) {
                return null;
            }

            if (signature.getRawType().equals(ARRAY)) {
                return ((List<?>) value).stream()
                        .map(v -> transformValue(signature.getArguments().get(0).getTypeSignature(), v))
                        .collect(Collectors.toList());
            }

            if (signature.getRawType().equals(MAP)) {
                ClientTypeSignature keySignature = signature.getArguments().get(0).getTypeSignature();
                ClientTypeSignature valueSignature = signature.getArguments().get(1).getTypeSignature();
                return ((Map<?, ?>) value).entrySet().stream()
                        .collect(Collectors.toMap(
                                e -> transformValue(keySignature, e.getKey()),
                                e -> transformValue(valueSignature, e.getValue())));
            }

            if (signature.getRawType().equals(ROW)) {
                Map<String, ClientTypeSignature> fieldSignatures = new HashMap<>();
                List<ClientTypeSignatureParameter> arguments = signature.getArguments();
                for (int i = 0; i < arguments.size(); i++) {
                    NamedTypeSignature namedTypeSignature = arguments.get(i).getNamedTypeSignature();
                    fieldSignatures.put(
                            namedTypeSignature.getName().orElse("field" + i),
                            new ClientTypeSignature(namedTypeSignature.getTypeSignature()));
                }
                return ((Map<?, ?>) value).entrySet().stream()
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                e -> transformValue(fieldSignatures.get(e.getKey()), e.getValue())));
            }

            if (!signature.isEnum()) {
                return value;
            }

            ClientTypeSignatureParameter parameter = signature.getArguments().get(0);
            if (ParameterKind.LONG_ENUM.equals(parameter.getKind())) {
                return parameter.getLongEnumMap().getEnumMapFlipped().getOrDefault(
                        ((Number) value).longValue(), value.toString());
            }
            if (ParameterKind.VARCHAR_ENUM.equals(parameter.getKind())) {
                return parameter.getVarcharEnumMap().getEnumMapFlipped().getOrDefault(
                        (String) value, (String) value);
            }
            return value;
        }
    }
}
