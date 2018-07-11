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
package com.facebook.presto.hive.benchmark;

import com.facebook.presto.hive.FileFormatDataSourceStats;
import com.facebook.presto.hive.GenericHiveRecordCursorProvider;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HiveCompressionCodec;
import com.facebook.presto.hive.HivePageSourceFactory;
import com.facebook.presto.hive.HiveRecordCursorProvider;
import com.facebook.presto.hive.HiveStorageFormat;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.hive.HiveTypeName;
import com.facebook.presto.hive.HiveTypeTranslator;
import com.facebook.presto.hive.RecordFileWriter;
import com.facebook.presto.hive.TypeTranslator;
import com.facebook.presto.hive.benchmark.HiveFileFormatBenchmark.TestData;
import com.facebook.presto.hive.orc.DwrfPageSourceFactory;
import com.facebook.presto.hive.orc.OrcPageSourceFactory;
import com.facebook.presto.hive.parquet.ParquetPageSourceFactory;
import com.facebook.presto.hive.parquet.ParquetRecordCursorProvider;
import com.facebook.presto.hive.rcfile.RcFilePageSourceFactory;
import com.facebook.presto.orc.OrcWriter;
import com.facebook.presto.orc.OrcWriterOptions;
import com.facebook.presto.orc.OrcWriterStats;
import com.facebook.presto.orc.OutputStreamOrcDataSink;
import com.facebook.presto.rcfile.AircompressorCodecFactory;
import com.facebook.presto.rcfile.HadoopCodecFactory;
import com.facebook.presto.rcfile.RcFileEncoding;
import com.facebook.presto.rcfile.RcFileWriter;
import com.facebook.presto.rcfile.binary.BinaryRcFileEncoding;
import com.facebook.presto.rcfile.text.TextRcFileEncoding;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordPageSource;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.OutputStreamSliceOutput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.joda.time.DateTimeZone;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static com.facebook.presto.hive.HdfsConfigurationUpdater.configureCompression;
import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.hive.HiveTestUtils.TYPE_MANAGER;
import static com.facebook.presto.hive.HiveType.toHiveType;
import static com.facebook.presto.hive.metastore.StorageFormat.fromHiveStorageFormat;
import static com.facebook.presto.orc.OrcEncoding.DWRF;
import static com.facebook.presto.orc.OrcEncoding.ORC;
import static com.facebook.presto.orc.OrcWriteValidation.OrcWriteValidationMode.BOTH;
import static java.util.stream.Collectors.joining;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.FILE_INPUT_FORMAT;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMNS;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMN_TYPES;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB;

public enum FileFormat
{
    PRESTO_RCBINARY {
        @Override
        public ConnectorPageSource createFileFormatReader(ConnectorSession session, HdfsEnvironment hdfsEnvironment, File targetFile, List<String> columnNames, List<Type> columnTypes)
        {
            HivePageSourceFactory pageSourceFactory = new RcFilePageSourceFactory(TYPE_MANAGER, hdfsEnvironment, new FileFormatDataSourceStats());
            return createPageSource(pageSourceFactory, session, targetFile, columnNames, columnTypes, HiveStorageFormat.RCBINARY);
        }

        @Override
        public FormatWriter createFileFormatWriter(
                ConnectorSession session,
                File targetFile,
                List<String> columnNames,
                List<Type> columnTypes,
                HiveCompressionCodec compressionCodec)
                throws IOException
        {
            return new PrestoRcFileFormatWriter(
                    targetFile,
                    columnTypes,
                    new BinaryRcFileEncoding(),
                    compressionCodec);
        }
    },

    PRESTO_RCTEXT {
        @Override
        public ConnectorPageSource createFileFormatReader(ConnectorSession session, HdfsEnvironment hdfsEnvironment, File targetFile, List<String> columnNames, List<Type> columnTypes)
        {
            HivePageSourceFactory pageSourceFactory = new RcFilePageSourceFactory(TYPE_MANAGER, hdfsEnvironment, new FileFormatDataSourceStats());
            return createPageSource(pageSourceFactory, session, targetFile, columnNames, columnTypes, HiveStorageFormat.RCTEXT);
        }

        @Override
        public FormatWriter createFileFormatWriter(
                ConnectorSession session,
                File targetFile,
                List<String> columnNames,
                List<Type> columnTypes,
                HiveCompressionCodec compressionCodec)
                throws IOException
        {
            return new PrestoRcFileFormatWriter(
                    targetFile,
                    columnTypes,
                    new TextRcFileEncoding(DateTimeZone.forID(session.getTimeZoneKey().getId())),
                    compressionCodec);
        }
    },

    PRESTO_ORC {
        @Override
        public ConnectorPageSource createFileFormatReader(ConnectorSession session, HdfsEnvironment hdfsEnvironment, File targetFile, List<String> columnNames, List<Type> columnTypes)
        {
            HivePageSourceFactory pageSourceFactory = new OrcPageSourceFactory(TYPE_MANAGER, false, hdfsEnvironment, new FileFormatDataSourceStats());
            return createPageSource(pageSourceFactory, session, targetFile, columnNames, columnTypes, HiveStorageFormat.ORC);
        }

        @Override
        public FormatWriter createFileFormatWriter(
                ConnectorSession session,
                File targetFile,
                List<String> columnNames,
                List<Type> columnTypes,
                HiveCompressionCodec compressionCodec)
                throws IOException
        {
            return new PrestoOrcFormatWriter(
                    targetFile,
                    columnNames,
                    columnTypes,
                    DateTimeZone.forID(session.getTimeZoneKey().getId()),
                    compressionCodec);
        }
    },

    PRESTO_DWRF {
        @Override
        public ConnectorPageSource createFileFormatReader(ConnectorSession session, HdfsEnvironment hdfsEnvironment, File targetFile, List<String> columnNames, List<Type> columnTypes)
        {
            HivePageSourceFactory pageSourceFactory = new DwrfPageSourceFactory(TYPE_MANAGER, hdfsEnvironment, new FileFormatDataSourceStats());
            return createPageSource(pageSourceFactory, session, targetFile, columnNames, columnTypes, HiveStorageFormat.DWRF);
        }

        @Override
        public FormatWriter createFileFormatWriter(
                ConnectorSession session,
                File targetFile,
                List<String> columnNames,
                List<Type> columnTypes,
                HiveCompressionCodec compressionCodec)
                throws IOException
        {
            return new PrestoDwrfFormatWriter(
                    targetFile,
                    columnNames,
                    columnTypes,
                    DateTimeZone.forID(session.getTimeZoneKey().getId()),
                    compressionCodec);
        }

        @Override
        public boolean supportsDate()
        {
            return false;
        }
    },

    PRESTO_PARQUET {
        @Override
        public ConnectorPageSource createFileFormatReader(ConnectorSession session, HdfsEnvironment hdfsEnvironment, File targetFile, List<String> columnNames, List<Type> columnTypes)
        {
            HivePageSourceFactory pageSourceFactory = new ParquetPageSourceFactory(TYPE_MANAGER, hdfsEnvironment, new FileFormatDataSourceStats());
            return createPageSource(pageSourceFactory, session, targetFile, columnNames, columnTypes, HiveStorageFormat.PARQUET);
        }

        @Override
        public FormatWriter createFileFormatWriter(
                ConnectorSession session,
                File targetFile,
                List<String> columnNames,
                List<Type> columnTypes,
                HiveCompressionCodec compressionCodec)
        {
            return new RecordFormatWriter(targetFile, columnNames, columnTypes, compressionCodec, HiveStorageFormat.PARQUET);
        }
    },

    HIVE_RCBINARY {
        @Override
        public ConnectorPageSource createFileFormatReader(ConnectorSession session, HdfsEnvironment hdfsEnvironment, File targetFile, List<String> columnNames, List<Type> columnTypes)
        {
            HiveRecordCursorProvider cursorProvider = new GenericHiveRecordCursorProvider(hdfsEnvironment);
            return createPageSource(cursorProvider, session, targetFile, columnNames, columnTypes, HiveStorageFormat.RCBINARY);
        }

        @Override
        public FormatWriter createFileFormatWriter(
                ConnectorSession session,
                File targetFile,
                List<String> columnNames,
                List<Type> columnTypes,
                HiveCompressionCodec compressionCodec)
        {
            return new RecordFormatWriter(targetFile, columnNames, columnTypes, compressionCodec, HiveStorageFormat.RCBINARY);
        }
    },

    HIVE_RCTEXT {
        @Override
        public ConnectorPageSource createFileFormatReader(ConnectorSession session, HdfsEnvironment hdfsEnvironment, File targetFile, List<String> columnNames, List<Type> columnTypes)
        {
            HiveRecordCursorProvider cursorProvider = new GenericHiveRecordCursorProvider(hdfsEnvironment);
            return createPageSource(cursorProvider, session, targetFile, columnNames, columnTypes, HiveStorageFormat.RCTEXT);
        }

        @Override
        public FormatWriter createFileFormatWriter(
                ConnectorSession session,
                File targetFile,
                List<String> columnNames,
                List<Type> columnTypes,
                HiveCompressionCodec compressionCodec)
        {
            return new RecordFormatWriter(targetFile, columnNames, columnTypes, compressionCodec, HiveStorageFormat.RCTEXT);
        }
    },

    HIVE_ORC {
        @Override
        public ConnectorPageSource createFileFormatReader(ConnectorSession session, HdfsEnvironment hdfsEnvironment, File targetFile, List<String> columnNames, List<Type> columnTypes)
        {
            HiveRecordCursorProvider cursorProvider = new GenericHiveRecordCursorProvider(hdfsEnvironment);
            return createPageSource(cursorProvider, session, targetFile, columnNames, columnTypes, HiveStorageFormat.ORC);
        }

        @Override
        public FormatWriter createFileFormatWriter(
                ConnectorSession session,
                File targetFile,
                List<String> columnNames,
                List<Type> columnTypes,
                HiveCompressionCodec compressionCodec)
        {
            return new RecordFormatWriter(targetFile, columnNames, columnTypes, compressionCodec, HiveStorageFormat.ORC);
        }
    },

    HIVE_DWRF {
        @Override
        public ConnectorPageSource createFileFormatReader(ConnectorSession session, HdfsEnvironment hdfsEnvironment, File targetFile, List<String> columnNames, List<Type> columnTypes)
        {
            HiveRecordCursorProvider cursorProvider = new GenericHiveRecordCursorProvider(hdfsEnvironment);
            return createPageSource(cursorProvider, session, targetFile, columnNames, columnTypes, HiveStorageFormat.DWRF);
        }

        @Override
        public FormatWriter createFileFormatWriter(
                ConnectorSession session,
                File targetFile,
                List<String> columnNames,
                List<Type> columnTypes,
                HiveCompressionCodec compressionCodec)
        {
            return new RecordFormatWriter(targetFile, columnNames, columnTypes, compressionCodec, HiveStorageFormat.DWRF);
        }

        @Override
        public boolean supportsDate()
        {
            return false;
        }
    },

    HIVE_PARQUET {
        @Override
        public ConnectorPageSource createFileFormatReader(ConnectorSession session, HdfsEnvironment hdfsEnvironment, File targetFile, List<String> columnNames, List<Type> columnTypes)
        {
            HiveRecordCursorProvider cursorProvider = new ParquetRecordCursorProvider(hdfsEnvironment, new FileFormatDataSourceStats());
            return createPageSource(cursorProvider, session, targetFile, columnNames, columnTypes, HiveStorageFormat.PARQUET);
        }

        @Override
        public FormatWriter createFileFormatWriter(
                ConnectorSession session,
                File targetFile,
                List<String> columnNames,
                List<Type> columnTypes,
                HiveCompressionCodec compressionCodec)
        {
            return new RecordFormatWriter(targetFile, columnNames, columnTypes, compressionCodec, HiveStorageFormat.PARQUET);
        }
    };

    public boolean supportsDate()
    {
        return true;
    }

    public abstract ConnectorPageSource createFileFormatReader(
            ConnectorSession session,
            HdfsEnvironment hdfsEnvironment,
            File targetFile,
            List<String> columnNames,
            List<Type> columnTypes);

    public abstract FormatWriter createFileFormatWriter(
            ConnectorSession session,
            File targetFile,
            List<String> columnNames,
            List<Type> columnTypes,
            HiveCompressionCodec compressionCodec)
            throws IOException;

    private static final JobConf conf;

    static {
        conf = new JobConf(new Configuration(false));
        conf.set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem");
    }

    public boolean supports(TestData testData)
    {
        return true;
    }

    private static ConnectorPageSource createPageSource(
            HiveRecordCursorProvider cursorProvider,
            ConnectorSession session, File targetFile, List<String> columnNames, List<Type> columnTypes, HiveStorageFormat format)
    {
        List<HiveColumnHandle> columnHandles = new ArrayList<>(columnNames.size());
        TypeTranslator typeTranslator = new HiveTypeTranslator();
        for (int i = 0; i < columnNames.size(); i++) {
            String columnName = columnNames.get(i);
            Type columnType = columnTypes.get(i);
            columnHandles.add(new HiveColumnHandle(columnName, toHiveType(typeTranslator, columnType), columnType.getTypeSignature(), i, REGULAR, Optional.empty()));
        }

        RecordCursor recordCursor = cursorProvider
                .createRecordCursor(
                        conf,
                        session,
                        new Path(targetFile.getAbsolutePath()),
                        0,
                        targetFile.length(),
                        targetFile.length(),
                        createSchema(format, columnNames, columnTypes),
                        columnHandles,
                        TupleDomain.all(),
                        DateTimeZone.forID(session.getTimeZoneKey().getId()),
                        TYPE_MANAGER)
                .get();
        return new RecordPageSource(columnTypes, recordCursor);
    }

    private static ConnectorPageSource createPageSource(
            HivePageSourceFactory pageSourceFactory,
            ConnectorSession session,
            File targetFile,
            List<String> columnNames,
            List<Type> columnTypes,
            HiveStorageFormat format)
    {
        List<HiveColumnHandle> columnHandles = new ArrayList<>(columnNames.size());
        TypeTranslator typeTranslator = new HiveTypeTranslator();
        for (int i = 0; i < columnNames.size(); i++) {
            String columnName = columnNames.get(i);
            Type columnType = columnTypes.get(i);
            columnHandles.add(new HiveColumnHandle(columnName, toHiveType(typeTranslator, columnType), columnType.getTypeSignature(), i, REGULAR, Optional.empty()));
        }

        return pageSourceFactory
                .createPageSource(
                        conf,
                        session,
                        new Path(targetFile.getAbsolutePath()),
                        0,
                        targetFile.length(),
                        targetFile.length(),
                        createSchema(format, columnNames, columnTypes),
                        columnHandles,
                        TupleDomain.all(),
                        DateTimeZone.forID(session.getTimeZoneKey().getId()))
                .get();
    }

    private static class RecordFormatWriter
            implements FormatWriter
    {
        private final RecordFileWriter recordWriter;

        public RecordFormatWriter(File targetFile,
                List<String> columnNames,
                List<Type> columnTypes,
                HiveCompressionCodec compressionCodec,
                HiveStorageFormat format)
        {
            JobConf config = new JobConf(conf);
            configureCompression(config, compressionCodec);

            recordWriter = new RecordFileWriter(
                    new Path(targetFile.toURI()),
                    columnNames,
                    fromHiveStorageFormat(format),
                    createSchema(format, columnNames, columnTypes),
                    format.getEstimatedWriterSystemMemoryUsage(),
                    config,
                    TYPE_MANAGER);
        }

        @Override
        public void writePage(Page page)
        {
            for (int position = 0; position < page.getPositionCount(); position++) {
                recordWriter.appendRow(page, position);
            }
        }

        @Override
        public void close()
        {
            recordWriter.commit();
        }
    }

    private static Properties createSchema(HiveStorageFormat format, List<String> columnNames, List<Type> columnTypes)
    {
        Properties schema = new Properties();
        TypeTranslator typeTranslator = new HiveTypeTranslator();
        schema.setProperty(SERIALIZATION_LIB, format.getSerDe());
        schema.setProperty(FILE_INPUT_FORMAT, format.getInputFormat());
        schema.setProperty(META_TABLE_COLUMNS, columnNames.stream()
                .collect(joining(",")));
        schema.setProperty(META_TABLE_COLUMN_TYPES, columnTypes.stream()
                .map(type -> toHiveType(typeTranslator, type))
                .map(HiveType::getHiveTypeName)
                .map(HiveTypeName::toString)
                .collect(joining(":")));
        return schema;
    }

    private static class PrestoRcFileFormatWriter
            implements FormatWriter
    {
        private final RcFileWriter writer;

        public PrestoRcFileFormatWriter(File targetFile, List<Type> types, RcFileEncoding encoding, HiveCompressionCodec compressionCodec)
                throws IOException
        {
            writer = new RcFileWriter(
                    new OutputStreamSliceOutput(new FileOutputStream(targetFile)),
                    types,
                    encoding,
                    compressionCodec.getCodec().map(Class::getName),
                    new AircompressorCodecFactory(new HadoopCodecFactory(getClass().getClassLoader())),
                    ImmutableMap.of(),
                    true);
        }

        @Override
        public void writePage(Page page)
                throws IOException
        {
            writer.write(page);
        }

        @Override
        public void close()
                throws IOException
        {
            writer.close();
        }
    }

    private static class PrestoOrcFormatWriter
            implements FormatWriter
    {
        private final OrcWriter writer;

        public PrestoOrcFormatWriter(File targetFile, List<String> columnNames, List<Type> types, DateTimeZone hiveStorageTimeZone, HiveCompressionCodec compressionCodec)
                throws IOException
        {
            writer = new OrcWriter(
                    new OutputStreamOrcDataSink(new FileOutputStream(targetFile)),
                    columnNames,
                    types,
                    ORC,
                    compressionCodec.getOrcCompressionKind(),
                    new OrcWriterOptions(),
                    ImmutableMap.of(),
                    hiveStorageTimeZone,
                    false,
                    BOTH,
                    new OrcWriterStats());
        }

        @Override
        public void writePage(Page page)
                throws IOException
        {
            writer.write(page);
        }

        @Override
        public void close()
                throws IOException
        {
            writer.close();
        }
    }

    private static class PrestoDwrfFormatWriter
            implements FormatWriter
    {
        private final OrcWriter writer;

        public PrestoDwrfFormatWriter(File targetFile, List<String> columnNames, List<Type> types, DateTimeZone hiveStorageTimeZone, HiveCompressionCodec compressionCodec)
                throws IOException
        {
            writer = new OrcWriter(
                    new OutputStreamOrcDataSink(new FileOutputStream(targetFile)),
                    columnNames,
                    types,
                    DWRF,
                    compressionCodec.getOrcCompressionKind(),
                    new OrcWriterOptions(),
                    ImmutableMap.of(),
                    hiveStorageTimeZone,
                    false,
                    BOTH,
                    new OrcWriterStats());
        }

        @Override
        public void writePage(Page page)
                throws IOException
        {
            writer.write(page);
        }

        @Override
        public void close()
                throws IOException
        {
            writer.close();
        }
    }
}
