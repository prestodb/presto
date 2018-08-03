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
package com.facebook.presto.hive.util;

import com.facebook.presto.orc.OrcWriteValidation.OrcWriteValidationMode;
import com.facebook.presto.orc.OrcWriter;
import com.facebook.presto.orc.OrcWriterOptions;
import com.facebook.presto.orc.OrcWriterStats;
import com.facebook.presto.orc.OutputStreamOrcDataSink;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.stream.IntStream;

import static com.facebook.presto.orc.OrcEncoding.ORC;
import static com.facebook.presto.orc.metadata.CompressionKind.LZ4;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static org.joda.time.DateTimeZone.UTC;

public class TempFileWriter
        implements Closeable
{
    private final OrcWriter orcWriter;

    public TempFileWriter(List<Type> types, OutputStream output)
    {
        this.orcWriter = createOrcFileWriter(output, types);
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

    private static OrcWriter createOrcFileWriter(OutputStream output, List<Type> types)
    {
        List<String> columnNames = IntStream.range(0, types.size())
                .mapToObj(String::valueOf)
                .collect(toImmutableList());

        return new OrcWriter(
                new OutputStreamOrcDataSink(output),
                columnNames,
                types,
                ORC,
                LZ4,
                new OrcWriterOptions()
                        .withMaxStringStatisticsLimit(new DataSize(0, BYTE))
                        .withStripeMinSize(new DataSize(4, MEGABYTE))
                        .withStripeMaxSize(new DataSize(4, MEGABYTE))
                        .withDictionaryMaxMemory(new DataSize(1, MEGABYTE)),
                ImmutableMap.of(),
                UTC,
                false,
                OrcWriteValidationMode.BOTH,
                new OrcWriterStats());
    }
}
