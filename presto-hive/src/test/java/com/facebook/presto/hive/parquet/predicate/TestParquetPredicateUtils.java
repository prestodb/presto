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
package com.facebook.presto.hive.parquet.predicate;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.parquet.RichColumnDescriptor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.block.MethodHandleUtil.methodHandle;
import static com.facebook.presto.common.predicate.TupleDomain.withColumnDomains;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.hive.parquet.ParquetPageSourceFactory.getParquetTupleDomain;
import static com.facebook.presto.parquet.ParquetTypeUtils.getDescriptors;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestParquetPredicateUtils
{
    @Test
    public void testParquetTupleDomainPrimitiveArray()
    {
        HiveColumnHandle columnHandle = new HiveColumnHandle("my_array", HiveType.valueOf("array<int>"), parseTypeSignature(StandardTypes.ARRAY), 0, REGULAR, Optional.empty(), Optional.empty());
        TupleDomain<HiveColumnHandle> domain = withColumnDomains(ImmutableMap.of(columnHandle, Domain.notNull(new ArrayType(INTEGER))));

        MessageType fileSchema = new MessageType("hive_schema",
                new GroupType(OPTIONAL, "my_array",
                        new GroupType(REPEATED, "bag", new PrimitiveType(OPTIONAL, INT32, "array_element"))));

        Map<List<String>, RichColumnDescriptor> descriptorsByPath = getDescriptors(fileSchema, fileSchema);
        TupleDomain<ColumnDescriptor> tupleDomain = getParquetTupleDomain(descriptorsByPath, domain);
        assertTrue(tupleDomain.getDomains().get().isEmpty());
    }

    @Test
    public void testParquetTupleDomainStructArray()
    {
        HiveColumnHandle columnHandle = new HiveColumnHandle("my_array_struct", HiveType.valueOf("array<struct<a:int>>"), parseTypeSignature(StandardTypes.ARRAY), 0, REGULAR, Optional.empty(), Optional.empty());
        RowType.Field rowField = new RowType.Field(Optional.of("a"), INTEGER);
        RowType rowType = RowType.from(ImmutableList.of(rowField));
        TupleDomain<HiveColumnHandle> domain = withColumnDomains(ImmutableMap.of(columnHandle, Domain.notNull(new ArrayType(rowType))));

        MessageType fileSchema = new MessageType("hive_schema",
                new GroupType(OPTIONAL, "my_array_struct",
                        new GroupType(REPEATED, "bag",
                                new GroupType(OPTIONAL, "array_element", new PrimitiveType(OPTIONAL, INT32, "a")))));

        Map<List<String>, RichColumnDescriptor> descriptorsByPath = getDescriptors(fileSchema, fileSchema);
        TupleDomain<ColumnDescriptor> tupleDomain = getParquetTupleDomain(descriptorsByPath, domain);
        assertTrue(tupleDomain.getDomains().get().isEmpty());
    }

    @Test
    public void testParquetTupleDomainPrimitive()
    {
        HiveColumnHandle columnHandle = new HiveColumnHandle("my_primitive", HiveType.valueOf("bigint"), parseTypeSignature(StandardTypes.BIGINT), 0, REGULAR, Optional.empty(), Optional.empty());
        Domain singleValueDomain = Domain.singleValue(BIGINT, 123L);
        TupleDomain<HiveColumnHandle> domain = withColumnDomains(ImmutableMap.of(columnHandle, singleValueDomain));

        MessageType fileSchema = new MessageType("hive_schema", new PrimitiveType(OPTIONAL, INT64, "my_primitive"));

        Map<List<String>, RichColumnDescriptor> descriptorsByPath = getDescriptors(fileSchema, fileSchema);
        TupleDomain<ColumnDescriptor> tupleDomain = getParquetTupleDomain(descriptorsByPath, domain);

        assertEquals(tupleDomain.getDomains().get().size(), 1);
        ColumnDescriptor descriptor = tupleDomain.getDomains().get().keySet().iterator().next();
        assertEquals(descriptor.getPath().length, 1);
        assertEquals(descriptor.getPath()[0], "my_primitive");

        Domain predicateDomain = Iterables.getOnlyElement(tupleDomain.getDomains().get().values());
        assertEquals(predicateDomain, singleValueDomain);
    }

    @Test
    public void testParquetTupleDomainStruct()
    {
        HiveColumnHandle columnHandle = new HiveColumnHandle("my_struct", HiveType.valueOf("struct<a:int,b:int>"), parseTypeSignature(StandardTypes.ROW), 0, REGULAR, Optional.empty(), Optional.empty());
        RowType.Field rowField = new RowType.Field(Optional.of("my_struct"), INTEGER);
        RowType rowType = RowType.from(ImmutableList.of(rowField));
        TupleDomain<HiveColumnHandle> domain = withColumnDomains(ImmutableMap.of(columnHandle, Domain.notNull(rowType)));

        MessageType fileSchema = new MessageType("hive_schema",
                new GroupType(OPTIONAL, "my_struct",
                        new PrimitiveType(OPTIONAL, INT32, "a"),
                        new PrimitiveType(OPTIONAL, INT32, "b")));
        Map<List<String>, RichColumnDescriptor> descriptorsByPath = getDescriptors(fileSchema, fileSchema);
        TupleDomain<ColumnDescriptor> tupleDomain = getParquetTupleDomain(descriptorsByPath, domain);
        assertTrue(tupleDomain.getDomains().get().isEmpty());
    }

    @Test
    public void testParquetTupleDomainMap()
    {
        HiveColumnHandle columnHandle = new HiveColumnHandle("my_map", HiveType.valueOf("map<int,int>"), parseTypeSignature(StandardTypes.MAP), 0, REGULAR, Optional.empty(), Optional.empty());

        MapType mapType = new MapType(
                INTEGER,
                INTEGER,
                methodHandle(TestParquetPredicateUtils.class, "throwUnsupportedOperationException"),
                methodHandle(TestParquetPredicateUtils.class, "throwUnsupportedOperationException"));

        TupleDomain<HiveColumnHandle> domain = withColumnDomains(ImmutableMap.of(columnHandle, Domain.notNull(mapType)));

        MessageType fileSchema = new MessageType("hive_schema",
                new GroupType(OPTIONAL, "my_map",
                        new GroupType(REPEATED, "map",
                                new PrimitiveType(REQUIRED, INT32, "key"),
                                new PrimitiveType(OPTIONAL, INT32, "value"))));

        Map<List<String>, RichColumnDescriptor> descriptorsByPath = getDescriptors(fileSchema, fileSchema);
        TupleDomain<ColumnDescriptor> tupleDomain = getParquetTupleDomain(descriptorsByPath, domain);
        assertTrue(tupleDomain.getDomains().get().isEmpty());
    }

    public static void throwUnsupportedOperationException()
    {
        throw new UnsupportedOperationException();
    }
}
