package com.facebook.presto.iceberg;

import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HiveFileWriter;
import com.facebook.presto.parquet.FileParquetDataSource;
import com.facebook.presto.parquet.cache.MetadataReader;
import com.google.inject.Inject;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.Optional;

import static com.facebook.presto.iceberg.rest.IcebergRestTestUtil.SESSION;
import static com.google.common.io.Files.createTempDir;
import static java.util.UUID.randomUUID;
import static org.testng.Assert.fail;

public class TestIcebergFileWriter {

    @Inject
    IcebergFileWriterFactory icebergFileWriterFactory;

    @Inject
    public TestIcebergFileWriter(IcebergFileWriterFactory icebergFileWriterFactory)
    {
        this.icebergFileWriterFactory = icebergFileWriterFactory;
    }

    @Test
    public void testWriteParquetFileWithLogicalTypes()
    {
        File temporaryDirectory = createTempDir();
        File parquetFile = new File(temporaryDirectory, randomUUID().toString());

        Schema icebergSchema = new Schema(Types.NestedField.optional(0, "timestamp_micros", Types.TimestampType.withoutZone()));

        HiveFileWriter writer = icebergFileWriterFactory.createFileWriter(
                new Path(parquetFile.getPath()),
                icebergSchema,
                new JobConf(),
                SESSION,
                new HdfsContext(SESSION),
                FileFormat.PARQUET,
                MetricsConfig.getDefault());

        try {
            FileParquetDataSource dataSource = new FileParquetDataSource(parquetFile);
            ParquetMetadata parquetMetadata = MetadataReader.readFooter(
                    dataSource,
                    parquetFile.length(),
                    Optional.empty(),
                    false).getParquetMetadata();
            MessageType writtenSchema = parquetMetadata.getFileMetaData().getSchema();
        }
        catch (IOException e) {
            fail("failed to read back file metadata", e);
        }
    }
}
