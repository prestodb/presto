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
package com.facebook.presto.hive;

import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.TestingTypeManager;
import com.facebook.presto.common.type.TypeManager;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.mapred.RecordReader;
import org.joda.time.DateTimeZone;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.Properties;
import java.util.stream.Stream;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.PARTITION_KEY;
import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.hive.HiveType.HIVE_INT;
import static com.facebook.presto.hive.HiveType.HIVE_STRING;
import static com.facebook.presto.hive.S3SelectRecordCursor.updateSplitSchema;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.joining;
import static org.apache.hadoop.hive.serde.serdeConstants.LIST_COLUMNS;
import static org.apache.hadoop.hive.serde.serdeConstants.LIST_COLUMN_TYPES;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_DDL;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB;
import static org.testng.Assert.assertEquals;

public class TestS3SelectRecordCursor
{
    private static final String LAZY_SERDE_CLASS_NAME = LazySimpleSerDe.class.getName();

    private static final HiveColumnHandle ARTICLE_COLUMN = new HiveColumnHandle("article", HIVE_STRING, parseTypeSignature(StandardTypes.VARCHAR), 1, REGULAR, Optional.empty(), Optional.empty());
    private static final HiveColumnHandle AUTHOR_COLUMN = new HiveColumnHandle("author", HIVE_STRING, parseTypeSignature(StandardTypes.VARCHAR), 1, REGULAR, Optional.empty(), Optional.empty());
    private static final HiveColumnHandle DATE_ARTICLE_COLUMN = new HiveColumnHandle("date_pub", HIVE_INT, parseTypeSignature(StandardTypes.DATE), 1, REGULAR, Optional.empty(), Optional.empty());
    private static final HiveColumnHandle QUANTITY_COLUMN = new HiveColumnHandle("quantity", HIVE_INT, parseTypeSignature(StandardTypes.INTEGER), 1, REGULAR, Optional.empty(), Optional.empty());
    private static final HiveColumnHandle[] DEFAULT_TEST_COLUMNS = {ARTICLE_COLUMN, AUTHOR_COLUMN, DATE_ARTICLE_COLUMN, QUANTITY_COLUMN};
    private static final HiveColumnHandle MOCK_HIVE_COLUMN_HANDLE = new HiveColumnHandle("mockName", HiveType.HIVE_FLOAT, parseTypeSignature(StandardTypes.DOUBLE), 88, PARTITION_KEY, Optional.empty(), Optional.empty());
    private static final TypeManager MOCK_TYPE_MANAGER = new TestingTypeManager();
    private static final Path MOCK_PATH = new Path("mockPath");

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "splitSchema is null")
    public void shouldFailOnNullSplitSchema()
    {
        new S3SelectRecordCursor(
                new Configuration(),
                MOCK_PATH,
                MOCK_RECORD_READER,
                100L,
                null,
                singletonList(MOCK_HIVE_COLUMN_HANDLE),
                DateTimeZone.UTC,
                MOCK_TYPE_MANAGER);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "columns is null")
    public void shouldFailOnNullColumns()
    {
        new S3SelectRecordCursor(
                new Configuration(),
                MOCK_PATH,
                MOCK_RECORD_READER,
                100L,
                new Properties(),
                null,
                DateTimeZone.UTC,
                MOCK_TYPE_MANAGER);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Invalid Thrift DDL struct article \\{ \\}")
    public void shouldThrowIllegalArgumentExceptionWhenSerialDDLHasNoColumns()
    {
        String ddlSerializationValue = "struct article { }";
        buildSplitSchema(ddlSerializationValue, DEFAULT_TEST_COLUMNS);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Thrift DDL should start with struct")
    public void shouldThrowIllegalArgumentExceptionWhenSerialDDLNotStartingWithStruct()
    {
        String ddlSerializationValue = "foo article { varchar article varchar }";
        buildSplitSchema(ddlSerializationValue, DEFAULT_TEST_COLUMNS);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Invalid Thrift DDL struct article \\{varchar article\\}")
    public void shouldThrowIllegalArgumentExceptionWhenSerialDDLNotStartingWithStruct2()
    {
        String ddlSerializationValue = "struct article {varchar article}";
        buildSplitSchema(ddlSerializationValue, DEFAULT_TEST_COLUMNS);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Invalid Thrift DDL struct article varchar article varchar \\}")
    public void shouldThrowIllegalArgumentExceptionWhenMissingOpenStartStruct()
    {
        String ddlSerializationValue = "struct article varchar article varchar }";
        buildSplitSchema(ddlSerializationValue, DEFAULT_TEST_COLUMNS);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Invalid Thrift DDL struct article\\{varchar article varchar author date date_pub int quantity")
    public void shouldThrowIllegalArgumentExceptionWhenDDlFormatNotCorrect()
    {
        String ddlSerializationValue = "struct article{varchar article varchar author date date_pub int quantity";
        buildSplitSchema(ddlSerializationValue, DEFAULT_TEST_COLUMNS);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Invalid Thrift DDL struct article \\{ varchar article varchar author date date_pub int quantity ")
    public void shouldThrowIllegalArgumentExceptionWhenEndOfStructNotFound()
    {
        String ddlSerializationValue = "struct article { varchar article varchar author date date_pub int quantity ";
        buildSplitSchema(ddlSerializationValue, DEFAULT_TEST_COLUMNS);
    }

    @Test
    public void shouldFilterColumnsWhichDoesNotMatchInTheHiveTable()
    {
        String ddlSerializationValue = "struct article { varchar address varchar company date date_pub int quantity}";
        String expectedDDLSerialization = "struct article { date date_pub, int quantity}";
        assertEquals(buildSplitSchema(ddlSerializationValue, DEFAULT_TEST_COLUMNS),
                buildExpectedProperties(expectedDDLSerialization, DEFAULT_TEST_COLUMNS));
    }

    @Test
    public void shouldReturnOnlyQuantityColumnInTheDDl()
    {
        String ddlSerializationValue = "struct article { varchar address varchar company date date_pub int quantity}";
        String expectedDDLSerialization = "struct article { int quantity}";
        assertEquals(buildSplitSchema(ddlSerializationValue, ARTICLE_COLUMN, QUANTITY_COLUMN),
                buildExpectedProperties(expectedDDLSerialization, ARTICLE_COLUMN, QUANTITY_COLUMN));
    }

    @Test
    public void shouldReturnProperties()
    {
        String ddlSerializationValue = "struct article { varchar article varchar author date date_pub int quantity}";
        String expectedDDLSerialization = "struct article { varchar article, varchar author, date date_pub, int quantity}";
        assertEquals(buildSplitSchema(ddlSerializationValue, DEFAULT_TEST_COLUMNS),
                buildExpectedProperties(expectedDDLSerialization, DEFAULT_TEST_COLUMNS));
    }

    @Test
    public void shouldReturnPropertiesWithoutDoubleCommaInColumnsNameLastColumnNameWithEndStruct()
    {
        String ddlSerializationValue = "struct article { varchar article, varchar author, date date_pub, int quantity}";
        String expectedDDLSerialization = "struct article { varchar article, varchar author, date date_pub, int quantity}";
        assertEquals(buildSplitSchema(ddlSerializationValue, DEFAULT_TEST_COLUMNS),
                buildExpectedProperties(expectedDDLSerialization, DEFAULT_TEST_COLUMNS));
    }

    @Test
    public void shouldReturnPropertiesWithoutDoubleCommaInColumnsNameLastColumnNameWithoutEndStruct()
    {
        String ddlSerializationValue = "struct article { varchar article, varchar author, date date_pub, int quantity }";
        String expectedDDLSerialization = "struct article { varchar article, varchar author, date date_pub, int quantity}";
        assertEquals(buildSplitSchema(ddlSerializationValue, DEFAULT_TEST_COLUMNS),
                buildExpectedProperties(expectedDDLSerialization, DEFAULT_TEST_COLUMNS));
    }

    @Test
    public void shouldOnlyGetColumnTypeFromHiveObjectAndNotFromDDLSerialLastColumnNameWithEndStruct()
    {
        String ddlSerializationValue = "struct article { int article, double author, xxxx date_pub, int quantity}";
        String expectedDDLSerialization = "struct article { int article, double author, xxxx date_pub, int quantity}";
        assertEquals(buildSplitSchema(ddlSerializationValue, DEFAULT_TEST_COLUMNS),
                buildExpectedProperties(expectedDDLSerialization, DEFAULT_TEST_COLUMNS));
    }

    @Test
    public void shouldOnlyGetColumnTypeFromHiveObjectAndNotFromDDLSerialLastColumnNameWithoutEndStruct()
    {
        String ddlSerializationValue = "struct article { int article, double author, xxxx date_pub, int quantity }";
        String expectedDDLSerialization = "struct article { int article, double author, xxxx date_pub, int quantity}";
        assertEquals(buildSplitSchema(ddlSerializationValue, DEFAULT_TEST_COLUMNS),
                buildExpectedProperties(expectedDDLSerialization, DEFAULT_TEST_COLUMNS));
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void shouldThrowNullPointerExceptionWhenColumnsIsNull()
    {
        updateSplitSchema(new Properties(), null);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void shouldThrowNullPointerExceptionWhenSchemaIsNull()
    {
        updateSplitSchema(null, ImmutableList.of());
    }

    private Properties buildSplitSchema(String ddlSerializationValue, HiveColumnHandle... columns)
    {
        Properties properties = new Properties();
        properties.put(SERIALIZATION_LIB, LAZY_SERDE_CLASS_NAME);
        properties.put(SERIALIZATION_DDL, ddlSerializationValue);
        return updateSplitSchema(properties, asList(columns));
    }

    private Properties buildExpectedProperties(String expectedDDLSerialization, HiveColumnHandle... expectedColumns)
    {
        String expectedColumnsType = getTypes(expectedColumns);
        String expectedColumnsName = getName(expectedColumns);
        Properties propExpected = new Properties();
        propExpected.put(LIST_COLUMNS, expectedColumnsName);
        propExpected.put(SERIALIZATION_LIB, LAZY_SERDE_CLASS_NAME);
        propExpected.put(SERIALIZATION_DDL, expectedDDLSerialization);
        propExpected.put(LIST_COLUMN_TYPES, expectedColumnsType);
        return propExpected;
    }

    private String getName(HiveColumnHandle[] expectedColumns)
    {
        return Stream.of(expectedColumns)
                .map(HiveColumnHandle::getName)
                .collect(joining(","));
    }

    private String getTypes(HiveColumnHandle[] expectedColumns)
    {
        return Stream.of(expectedColumns)
                .map(HiveColumnHandle::getHiveType)
                .map(HiveType::getTypeInfo)
                .map(TypeInfo::getTypeName)
                .collect(joining(","));
    }

    private static final RecordReader<?, ?> MOCK_RECORD_READER = new RecordReader()
    {
        @Override
        public boolean next(Object key, Object value)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object createKey()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object createValue()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getPos()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public float getProgress()
        {
            throw new UnsupportedOperationException();
        }
    };
}
