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
package com.facebook.presto.hive.parquet;

import com.facebook.presto.hive.HiveQueryRunner;
import com.facebook.presto.hive.parquet.write.TestMapredParquetOutputFormat;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaHiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.parquet.schema.MessageType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static com.facebook.presto.common.type.Decimals.overflows;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterators.transform;
import static java.lang.Integer.MAX_VALUE;
import static java.lang.String.format;
import static java.math.RoundingMode.UNNECESSARY;
import static java.util.Arrays.stream;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardStructObjectInspector;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_2_0;
import static org.apache.parquet.hadoop.ParquetOutputFormat.COMPRESSION;
import static org.apache.parquet.hadoop.ParquetOutputFormat.ENABLE_DICTIONARY;
import static org.apache.parquet.hadoop.ParquetOutputFormat.WRITER_VERSION;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED;
import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;
import static org.assertj.core.api.Assertions.assertThat;

public class TestParquetDecimalScaling
        extends AbstractTestQueryFramework
{
    private java.nio.file.Path basePath;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        basePath = getBasePath();

        return HiveQueryRunner.createQueryRunner(
                ImmutableList.of(),
                ImmutableMap.of(),
                "sql-standard",
                ImmutableMap.of(),
                Optional.of(basePath));
    }

    @Test(dataProvider = "testParquetLongFixedLenByteArrayWithShortDecimalProvider")
    public void testParquetLongFixedLenByteArrayWithShortDecimal(int schemaPrecision, int schemaScale, int parquetPrecision, int parquetScale, String writeValue)
    {
        String tableName = generateTableName("rounded_decimals", parquetPrecision, parquetScale);
        createTable(tableName, schemaPrecision, schemaScale);

        int byteArrayLength = ParquetHiveSerDe.PRECISION_TO_BYTE_COUNT[parquetPrecision - 1];
        MessageType schema = parseMessageType(format(
                "message hive_record { optional fixed_len_byte_array(%d) value (DECIMAL(%d, %d)); }",
                byteArrayLength,
                schemaPrecision,
                schemaScale));
        List<ObjectInspector> inspectors = ImmutableList.of(new JavaHiveDecimalObjectInspector(new DecimalTypeInfo(parquetPrecision, parquetScale)));

        createParquetFile(
                getParquetWritePath(tableName),
                getStandardStructObjectInspector(ImmutableList.of("value"), inspectors),
                new Iterator[] {ImmutableList.of(HiveDecimal.create(writeValue)).stream().iterator()},
                schema,
                Collections.singletonList("hive_record"));

        if (overflows(new BigDecimal(writeValue).unscaledValue(), schemaPrecision)) {
            assertQueryFails(
                    format("SELECT * FROM tpch.%s", tableName),
                    format("Could not read fixed_len_byte_array\\(%d\\) value %s into decimal\\(%d,%d\\)", byteArrayLength, writeValue, schemaPrecision, schemaScale));
        }
        else {
            assertValues(tableName, schemaScale, ImmutableList.of(writeValue));
        }

        dropTable(tableName);
    }

    @DataProvider
    public Object[][] testParquetLongFixedLenByteArrayWithShortDecimalProvider()
    {
        // schemaPrecision, schemaScale, parquetPrecision, parquetScale, writeValue
        return new Object[][] {
                {5, 2, 19, 2, "-5"},
                {5, 2, 20, 2, "999.99"},
                {7, 2, 24, 2, "-99999.99"},
                {10, 2, 26, 2, "99999999.99"},
                {14, 4, 30, 4, "99999999.99"},
                {18, 8, 32, 8, "1234567890.12345678"},
                {18, 8, 32, 8, "123456789012.12345678"},
                {18, 8, 38, 8, "4989875563210.12345678"},
        };
    }

    protected void createTable(String tableName, int precision, int scale)
    {
        assertUpdate(format("CREATE TABLE tpch.%s (value decimal(%d, %d)) WITH (format = 'PARQUET')", tableName, precision, scale));
    }

    protected void dropTable(String tableName)
    {
        assertUpdate(format("DROP TABLE %s", tableName));
    }

    protected void assertValues(String tableName, int scale, List<String> expected)
    {
        MaterializedResult materializedRows = computeActual(format("SELECT value FROM tpch.%s", tableName));

        List<BigDecimal> actualValues = materializedRows.getMaterializedRows().stream()
                .map(row -> row.getField(0))
                .map(BigDecimal.class::cast)
                .collect(toImmutableList());

        BigDecimal[] expectedValues = expected.stream()
                .map(value -> new BigDecimal(value).setScale(scale, UNNECESSARY))
                .toArray(BigDecimal[]::new);

        assertThat(actualValues).containsExactlyInAnyOrder(expectedValues);
    }

    private static java.nio.file.Path getBasePath()
    {
        try {
            return Files.createTempDirectory("parquet");
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static Properties createTableProperties(List<String> columnNames, List<ObjectInspector> objectInspectors)
    {
        Properties tableProperties = new Properties();
        tableProperties.setProperty("columns", Joiner.on(',').join(columnNames));
        tableProperties.setProperty("columns.types", Joiner.on(',').join(transform(objectInspectors.iterator(), ObjectInspector::getTypeName)));
        return tableProperties;
    }

    private static String generateTableName(String testCase, int precision, int scale)
    {
        return format("%s_%d_%d_%d", testCase, precision, scale, ThreadLocalRandom.current().nextInt(1, MAX_VALUE));
    }

    private Path getParquetWritePath(String tableName)
    {
        return new Path(basePath.toString(), format("hive_data/tpch/%s/%s", tableName, UUID.randomUUID().toString()));
    }

    private static void createParquetFile(
            Path path,
            StandardStructObjectInspector inspector,
            Iterator<?>[] iterators,
            MessageType parquetSchema,
            List<String> columnNames)
    {
        Properties tableProperties = createTableProperties(columnNames, Collections.singletonList(inspector));

        JobConf jobConf = new JobConf();
        jobConf.setEnum(COMPRESSION, UNCOMPRESSED);
        jobConf.setBoolean(ENABLE_DICTIONARY, false);
        jobConf.setEnum(WRITER_VERSION, PARQUET_2_0);

        try {
            FileSinkOperator.RecordWriter recordWriter = new TestMapredParquetOutputFormat(Optional.of(parquetSchema), true)
                    .getHiveRecordWriter(
                            jobConf,
                            path,
                            Text.class,
                            false,
                            tableProperties,
                            () -> {});

            Object row = inspector.create();
            List<StructField> fields = ImmutableList.copyOf(inspector.getAllStructFieldRefs());

            while (stream(iterators).allMatch(Iterator::hasNext)) {
                for (int i = 0; i < fields.size(); i++) {
                    Object value = iterators[i].next();
                    inspector.setStructFieldData(row, fields.get(i), value);
                }

                ParquetHiveSerDe serde = new ParquetHiveSerDe();
                serde.initialize(jobConf, tableProperties, null);
                Writable record = serde.serialize(row, inspector);
                recordWriter.write(record);
            }

            recordWriter.close(false);
        }
        catch (IOException | SerDeException e) {
            throw new RuntimeException(e);
        }
    }
}
