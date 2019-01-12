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
package io.prestosql.plugin.hive.util;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.prestosql.orc.OrcDataSink;
import io.prestosql.orc.OrcWriteValidation.OrcWriteValidationMode;
import io.prestosql.orc.OrcWriter;
import io.prestosql.orc.OrcWriterOptions;
import io.prestosql.orc.OrcWriterStats;
import io.prestosql.spi.Page;
import io.prestosql.spi.type.Type;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.prestosql.orc.OrcEncoding.ORC;
import static io.prestosql.orc.metadata.CompressionKind.LZ4;
import static org.joda.time.DateTimeZone.UTC;

public class TempFileWriter
        implements Closeable
{
    private final OrcWriter orcWriter;

    public TempFileWriter(List<Type> types, OrcDataSink sink)
    {
        this.orcWriter = createOrcFileWriter(sink, types);
    }

    public void writePage(Page page)
    {
        try {
            orcWriter.write(page);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close()
            throws IOException
    {
        orcWriter.close();
    }

    public long getWrittenBytes()
    {
        return orcWriter.getWrittenBytes();
    }

    private static OrcWriter createOrcFileWriter(OrcDataSink sink, List<Type> types)
    {
        List<String> columnNames = IntStream.range(0, types.size())
                .mapToObj(String::valueOf)
                .collect(toImmutableList());

        return new OrcWriter(
                sink,
                columnNames,
                types,
                ORC,
                LZ4,
                new OrcWriterOptions()
                        .withMaxStringStatisticsLimit(new DataSize(0, BYTE))
                        .withStripeMinSize(new DataSize(64, MEGABYTE))
                        .withDictionaryMaxMemory(new DataSize(1, MEGABYTE)),
                ImmutableMap.of(),
                UTC,
                false,
                OrcWriteValidationMode.BOTH,
                new OrcWriterStats());
    }
}
