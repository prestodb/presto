package com.facebook.presto.hive;

import com.facebook.presto.hive.util.TempFileWriter;
import com.facebook.presto.orc.OrcWriteValidation;
import com.facebook.presto.orc.OrcWriter;
import com.facebook.presto.orc.OrcWriterOptions;
import com.facebook.presto.orc.OrcWriterStats;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.inject.Inject;

import java.io.IOException;
import java.util.List;
import java.util.stream.IntStream;

import static com.facebook.presto.orc.OrcEncoding.ORC;
import static com.facebook.presto.orc.metadata.CompressionKind.LZ4;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Objects.requireNonNull;
import static org.joda.time.DateTimeZone.UTC;

public class TempFileWriterFactory
{
    private final OrcFileWriterFactory orcFileWriterFactory;

    @Inject
    public TempFileWriterFactory(OrcFileWriterFactory orcFileWriterFactory)
    {
        this.orcFileWriterFactory = requireNonNull(orcFileWriterFactory, "orcFileWriterFactory is null");
    }

    TempFileWriter createTempFileWriter(ConnectorSession session, List<Type> types, FileSystem fileSystem, Path path)
            throws IOException
    {
        List<String> columnNames = IntStream.range(0, types.size())
                .mapToObj(String::valueOf)
                .collect(toImmutableList());

        return new TempFileWriter(
                new OrcWriter(
                        orcFileWriterFactory.createOrcDataSink(session, fileSystem, path),
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
                        OrcWriteValidation.OrcWriteValidationMode.BOTH,
                        new OrcWriterStats()));
    }
}
