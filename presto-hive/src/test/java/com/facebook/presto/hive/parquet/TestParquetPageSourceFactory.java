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

import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.hive.BaseHiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.hive.BaseHiveColumnHandle.ColumnType.SYNTHESIZED;
import static com.facebook.presto.hive.parquet.ParquetPageSourceFactory.getColumnType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestParquetPageSourceFactory
{
    @Test
    public void testGetColumnType()
    {
        MessageType messageType = buildMessageType();
        SchemaTableName tableName = new SchemaTableName("db001", "tbl001");
        Path path = new Path("/tmp/hello");

        // Simple field by index.
        Optional<org.apache.parquet.schema.Type> parquetType = getColumnType(VarcharType.VARCHAR, messageType, false,
                buildSimpleColumnHandle("name", 0), tableName, path);
        PrimitiveType expectedNameType = new PrimitiveType(REQUIRED, BINARY, "name");
        assertTrue(parquetType.isPresent());
        assertEquals(
                parquetType.get(), expectedNameType);

        // Simple field by name.
        parquetType = getColumnType(VarcharType.VARCHAR, messageType, true,
                buildSimpleColumnHandle("name", 0), tableName, path);
        assertTrue(parquetType.isPresent());
        assertEquals(
                parquetType.get(), expectedNameType);

        // Pushdown fields with useParquetColumnNames = false.
        HiveColumnHandle addressCityColumn = buildNestedPushDownColumnHandle("address", "city");
        parquetType = getColumnType(VarcharType.VARCHAR, messageType, false,
                addressCityColumn, tableName, path);

        PrimitiveType city = new PrimitiveType(REQUIRED, BINARY, "city");
        MessageType expectedAddressWithCityType = new MessageType("address", ImmutableList.of(city));

        assertTrue(parquetType.isPresent());
        assertEquals(
                parquetType.get(), expectedAddressWithCityType);

        // Pushdown fields with useParquetColumnNames = true.
        parquetType = getColumnType(VarcharType.VARCHAR, messageType, true,
                addressCityColumn, tableName, path);
        assertTrue(parquetType.isPresent());
        assertEquals(
                parquetType.get(), expectedAddressWithCityType);
    }

    private static HiveColumnHandle buildSimpleColumnHandle(String name, int index)
    {
        HiveColumnHandle column = new HiveColumnHandle(
                name,
                HiveType.HIVE_STRING,
                VarcharType.VARCHAR.getTypeSignature(),
                index,
                REGULAR,
                Optional.empty(),
                ImmutableList.of(),
                Optional.empty());
        return column;
    }

    private static HiveColumnHandle buildNestedPushDownColumnHandle(String field1, String field2)
    {
        Subfield subfield = new Subfield(field1, ImmutableList.of(new Subfield.NestedField(field2)));
        HiveColumnHandle column = new HiveColumnHandle(
                String.format("%s$_$_$%s", field1, field2),
                HiveType.HIVE_STRING,
                VarcharType.VARCHAR.getTypeSignature(),
                -1,
                SYNTHESIZED,
                Optional.of("nested column pushdown"),
                ImmutableList.of(subfield),
                Optional.empty());
        return column;
    }

    private static MessageType buildMessageType()
    {
        PrimitiveType name = new PrimitiveType(REQUIRED, BINARY, "name");
        PrimitiveType age = new PrimitiveType(REQUIRED, INT32, "age");

        PrimitiveType city = new PrimitiveType(REQUIRED, BINARY, "city");
        PrimitiveType block = new PrimitiveType(REQUIRED, BINARY, "block");
        GroupType address = new GroupType(REQUIRED, "address", ImmutableList.of(city, block));
        MessageType messageType = new MessageType("root", ImmutableList.of(name, age, address));
        return messageType;
    }
}
