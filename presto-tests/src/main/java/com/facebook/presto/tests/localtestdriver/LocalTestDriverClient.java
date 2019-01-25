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
package com.facebook.presto.tests.localtestdriver;

import com.facebook.presto.Session;
import com.facebook.presto.client.QueryData;
import com.facebook.presto.client.QueryStatusInfo;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.MapType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.tests.AbstractTestingPrestoClient;
import com.facebook.presto.tests.ResultWithQueryId;
import com.facebook.presto.tests.ResultsSession;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import io.airlift.log.Logger;
import org.intellij.lang.annotations.Language;

import java.io.BufferedWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.io.BaseEncoding.base16;
import static java.util.Objects.requireNonNull;

public class LocalTestDriverClient
        extends AbstractTestingPrestoClient<ResultInfo>
{
    private static final Logger log = Logger.get(LocalTestDriverClient.class);
    private static final int DEFAULT_PRECISION = 9;
    private static final int MAX_LINES_PER_FILE = 1000;
    private static final long PRIME64 = 0x9E3779B185EBCA87L;
    private static final Joiner HEX_BYTE_JOINER = Joiner.on(' ');
    private static final Splitter HEX_SPLITTER = Splitter.fixedLength(2);

    private final Map<QueryId, Optional<Path>> dataPathMap = new HashMap<>();

    public LocalTestDriverClient(TestingPrestoServer prestoServer)
    {
        super(prestoServer, testSessionBuilder().build());
    }

    @Override
    protected ResultsSession<ResultInfo> getResultSession(Session session)
    {
        Optional<Path> dataPath;
        synchronized (dataPathMap) {
            dataPath = dataPathMap.remove(session.getQueryId());
        }
        checkState(dataPath != null, "unknown session");
        return new StreamingResultSession(dataPath);
    }

    public ResultWithQueryId<ResultInfo> execute(Session session, @Language("SQL") String sql, Optional<Path> dataPath)
            throws IOException
    {
        synchronized (dataPathMap) {
            checkState(dataPathMap.put(session.getQueryId(), dataPath) == null, "duplicate queryId submitted");
        }
        return super.execute(session, sql);
    }

    @Override
    public ResultWithQueryId<ResultInfo> execute(Session session, @Language("SQL") String sql)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ResultWithQueryId<ResultInfo> execute(@Language("SQL") String sql)
    {
        throw new UnsupportedOperationException();
    }

    private class StreamingResultSession
            implements ResultsSession<ResultInfo>
    {
        private final AtomicBoolean loggedUri = new AtomicBoolean(false);
        private final AtomicReference<List<Type>> types = new AtomicReference<>();

        private final AtomicReference<Optional<String>> updateType = new AtomicReference<>(Optional.empty());
        private final AtomicReference<OptionalLong> updateCount = new AtomicReference<>(OptionalLong.empty());
        private final Optional<Path> dataPath;
        private final AtomicReference<List<Long>> checkSums = new AtomicReference<>();
        private final List<String> formattedRows = new ArrayList<>(MAX_LINES_PER_FILE);
        private long rowCount;

        public StreamingResultSession(Optional<Path> dataPath)
        {
            this.dataPath = requireNonNull(dataPath, "dataPath is null");
        }

        @Override
        public void setUpdateType(String type)
        {
            updateType.set(Optional.of(type));
        }

        @Override
        public void setUpdateCount(long count)
        {
            updateCount.set(OptionalLong.of(count));
        }

        @Override
        public void addResults(QueryStatusInfo statusInfo, QueryData data)
        {
            if (!loggedUri.getAndSet(true)) {
                log.info("Query %s: %s", statusInfo.getId(), statusInfo.getInfoUri());
            }
            if (types.get() == null && statusInfo.getColumns() != null) {
                types.set(getTypes(statusInfo.getColumns()));
                initializeChecksums();
            }
            if (data.getData() != null) {
                checkState(types.get() != null, "data received without types");
                for (List<?> row : data.getData()) {
                    processRow(row);
                }
            }
        }

        private void initializeChecksums()
        {
            if (checkSums.get() == null && types.get() != null) {
                checkSums.set(new ArrayList<>(types.get().size()));
                for (Type type : types.get()) {
                    checkSums.get().add(0L);
                }
            }
        }

        @Override
        public ResultInfo build(Map<String, String> setSessionProperties, Set<String> resetSessionProperties)
        {
            try {
                writeSortedRows();
                return new ResultInfo(updateType.get(), getUpdateCount(), checkSums.get(), rowCount);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private void writeSortedRows()
                throws IOException
        {
            if (rowCount <= MAX_LINES_PER_FILE && dataPath.isPresent()) {
                formattedRows.sort(Comparator.naturalOrder());
                try (BufferedWriter writer = Files.newBufferedWriter(dataPath.get())) {
                    for (String row : formattedRows) {
                        writer.write(row);
                    }
                }
            }
        }

        private Optional<Long> getUpdateCount()
        {
            if (updateCount.get().isPresent()) {
                return Optional.of(updateCount.get().getAsLong());
            }
            else {
                return Optional.empty();
            }
        }

        private void processRow(List<?> row)
        {
            StringBuilder sb = new StringBuilder();
            Iterator<?> iterator = row.iterator();
            Iterator<Type> typeIterator = types.get().iterator();
            verify(types.get().size() == checkSums.get().size(), "checksum size not equal to number of columns");
            // djb2 hash function: http://www.cse.yorku.ca/~oz/hash.html
            for (int columnIndex = 0; iterator.hasNext(); columnIndex++) {
                checkState(typeIterator.hasNext(), "Malformed row %s", row.toString());
                String s = formatValue(iterator.next(), typeIterator.next());
                long columnHash = 5381L;
                for (int i = 0; i < s.length(); i++) {
                    escapeCharacter(sb, s.charAt(i));
                    columnHash = columnHash * 33 ^ s.charAt(i);
                }
                checkSums.get().set(columnIndex, checkSums.get().get(columnIndex) + PRIME64 * columnHash);
                if (iterator.hasNext()) {
                    sb.append('\t');
                }
            }
            sb.append('\n');
            if (rowCount < MAX_LINES_PER_FILE) {
                formattedRows.add(sb.toString());
            }
            rowCount++;
        }

        private void escapeCharacter(StringBuilder builder, char c)
        {
            switch (c) {
                case '\0':
                    builder.append('\\').append('0');
                    break;
                case '\b':
                    builder.append('\\').append('b');
                    break;
                case '\f':
                    builder.append('\\').append('f');
                    break;
                case '\n':
                    builder.append('\\').append('n');
                    break;
                case '\r':
                    builder.append('\\').append('r');
                    break;
                case '\t':
                    builder.append('\\').append('t');
                    break;
                case '\\':
                    builder.append('\\').append('\\');
                    break;
                default:
                    builder.append(c);
            }
        }

        private String formatValue(Object value, Type type)
        {
            if (value == null) {
                return "";
            }

            if (value instanceof byte[]) {
                return formatHexDump((byte[]) value);
            }
            else if (value instanceof Double) {
                return new ApproximateDouble((Double) value, DEFAULT_PRECISION).toString();
            }
            else if (value instanceof Float) {
                return new ApproximateFloat((Float) value, DEFAULT_PRECISION).toString();
            }
            else if (type instanceof ArrayType) {
                List<String> serialized = new ArrayList<>(((List<Object>) value).size());
                for (Object element : (List<Object>) value) {
                    serialized.add(formatValue(element, ((ArrayType) type).getElementType()));
                }
                serialized.sort(Comparator.naturalOrder());
                return serialized.toString();
            }
            else if (type instanceof MapType) {
                Map<String, String> serialized = new TreeMap<>();
                Type keyType = ((MapType) type).getKeyType();
                Type valueType = ((MapType) type).getValueType();
                for (Map.Entry<Object, Object> entry : ((Map<Object, Object>) value).entrySet()) {
                    String entryKey = formatValue(entry.getKey(), keyType);
                    String entryValue = formatValue(entry.getValue(), valueType);
                    serialized.put(entryKey, entryValue);
                }
                return serialized.toString();
            }
            else {
                return value.toString();
            }
        }
        private String formatHexDump(byte[] bytes)
        {
            return HEX_BYTE_JOINER.join(createHexPairs(bytes));
        }

        private Iterable<String> createHexPairs(byte[] bytes)
        {
            // hex dump: "616263"
            String hexDump = base16().lowerCase().encode(bytes);

            // hex pairs: ["61", "62", "63"]
            return HEX_SPLITTER.split(hexDump);
        }
    }

    private abstract static class ApproximateNumeric
    {
        public abstract Number getValue();

        protected abstract Number getNormalizedValue();

        @Override
        public String toString()
        {
            return getNormalizedValue().toString();
        }

        @Override
        public boolean equals(Object obj)
        {
            if (obj == this) {
                return true;
            }
            if ((obj == null) || (getClass() != obj.getClass())) {
                return false;
            }

            ApproximateNumeric o = (ApproximateNumeric) obj;
            return Objects.equals(getNormalizedValue(), o.getNormalizedValue());
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(getNormalizedValue());
        }
    }

    private static class ApproximateDouble
            extends ApproximateNumeric
    {
        private final Double value;
        private final int precision;

        private ApproximateDouble(Double value, int precision)
        {
            this.value = requireNonNull(value, "value is null");
            this.precision = precision;
        }

        @Override
        public Number getValue()
        {
            return value;
        }

        @Override
        protected Number getNormalizedValue()
        {
            if (value.isNaN() || value.isInfinite()) {
                return value;
            }
            return new BigDecimal(getValue().doubleValue()).round(new MathContext(precision)).doubleValue();
        }
    }

    private static class ApproximateFloat
            extends ApproximateNumeric
    {
        private final Float value;
        private final int precision;

        private ApproximateFloat(Float value, int precision)
        {
            this.value = requireNonNull(value, "value is null");
            this.precision = precision;
        }

        @Override
        public Number getValue()
        {
            return value;
        }

        @Override
        protected Number getNormalizedValue()
        {
            if (value.isNaN() || value.isInfinite()) {
                return value;
            }
            return new BigDecimal(getValue().floatValue()).round(new MathContext(precision)).floatValue();
        }
    }
}
