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
package com.facebook.presto.parquet.reader;

import com.facebook.presto.parquet.ParquetDataSourceId;
import com.facebook.presto.parquet.cache.MetadataReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Optional;

import static com.facebook.presto.parquet.ParquetTypeUtils.getColumnIO;
import static com.facebook.presto.parquet.ParquetTypeUtils.lookupColumnByName;
import static org.apache.parquet.io.ColumnIOConverter.constructField;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestVariantColumnReader
{
    private static final Configuration CONF = new Configuration(false);

    /**
     * A shredded Variant group contains a third child {@code typed_value} alongside
     * the usual {@code metadata} and {@code value}. Reading such a file must throw
     * an explicit {@link IllegalArgumentException} rather than silently returning
     * null for every row.
     */
    @Test
    public void testShreddedVariantIsRejected()
            throws IOException
    {
        // Build a shredded Variant schema: metadata + value + typed_value (INT32)
        MessageType schema = new MessageType("root",
                Types.buildGroup(OPTIONAL)
                        .as(LogicalTypeAnnotation.variantType((byte) 1))
                        .required(BINARY).named("metadata")
                        .optional(BINARY).named("value")
                        .optional(INT32).named("typed_value")
                        .named("data"));

        java.io.File tmp = Files.createTempDirectory("test_shredded_variant").toFile();
        Path parquetPath = new Path(tmp.getAbsolutePath(), "shredded.parquet");

        Configuration conf = new Configuration(false);
        conf.set(GroupWriteSupport.PARQUET_EXAMPLE_SCHEMA, schema.toString());

        // Write one row so the file is valid Parquet
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(parquetPath)
                .withConf(conf)
                .withCompressionCodec(org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED)
                .build()) {
            SimpleGroup row = new SimpleGroup(schema);
            SimpleGroup dataGroup = new SimpleGroup((org.apache.parquet.schema.GroupType) schema.getType("data"));
            dataGroup.add("metadata", org.apache.parquet.io.api.Binary.fromConstantByteArray(new byte[]{0x01, 0x00, 0x00}));
            dataGroup.add("typed_value", 42);
            row.add("data", dataGroup);
            writer.write(row);
        }

        FileSystem fs = parquetPath.getFileSystem(conf);
        MockParquetDataSource dataSource = new MockParquetDataSource(
                new ParquetDataSourceId(parquetPath.toString()),
                fs.open(parquetPath));

        ParquetMetadata parquetMetadata = MetadataReader.readFooter(
                dataSource,
                fs.getFileStatus(parquetPath).getLen(),
                Optional.empty(),
                false).getParquetMetadata();
        MessageType fileSchema = parquetMetadata.getFileMetaData().getSchema();
        MessageColumnIO messageColumn = getColumnIO(fileSchema, fileSchema);

        try {
            constructField(
                    com.facebook.presto.common.type.JsonType.JSON,
                    lookupColumnByName(messageColumn, "data"));
            fail("Expected IllegalArgumentException for shredded Variant column");
        }
        catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Shredded Variant columns are not supported"),
                    "Unexpected message: " + e.getMessage());
        }
    }
}
