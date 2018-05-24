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

import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.hive.parquet.RichColumnDescriptor;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.MapType;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.StandardTypes;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.testng.annotations.Test;
import parquet.column.ColumnDescriptor;
import parquet.column.Encoding;
import parquet.schema.GroupType;
import parquet.schema.MessageType;
import parquet.schema.PrimitiveType;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.hive.parquet.ParquetTypeUtils.getDescriptors;
import static com.facebook.presto.hive.parquet.predicate.ParquetPredicateUtils.getParquetTupleDomain;
import static com.facebook.presto.hive.parquet.predicate.ParquetPredicateUtils.isOnlyDictionaryEncodingPages;
import static com.facebook.presto.spi.block.MethodHandleUtil.methodHandle;
import static com.facebook.presto.spi.predicate.TupleDomain.withColumnDomains;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.google.common.collect.Sets.union;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static parquet.column.Encoding.BIT_PACKED;
import static parquet.column.Encoding.PLAIN;
import static parquet.column.Encoding.PLAIN_DICTIONARY;
import static parquet.column.Encoding.RLE;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static parquet.schema.Type.Repetition.OPTIONAL;
import static parquet.schema.Type.Repetition.REPEATED;
import static parquet.schema.Type.Repetition.REQUIRED;

public class TestParquetPredicateUtils
{
    @Test
    @SuppressWarnings("deprecation")
    public void testDictionaryEncodingCasesV1()
    {
        Set<Encoding> required = ImmutableSet.of(BIT_PACKED);
        Set<Encoding> optional = ImmutableSet.of(BIT_PACKED, RLE);
        Set<Encoding> repeated = ImmutableSet.of(RLE);

        Set<Encoding> notDictionary = ImmutableSet.of(PLAIN);
        Set<Encoding> mixedDictionary = ImmutableSet.of(PLAIN_DICTIONARY, PLAIN);
        Set<Encoding> dictionary = ImmutableSet.of(PLAIN_DICTIONARY);

        assertFalse(isOnlyDictionaryEncodingPages(union(required, notDictionary)), "required notDictionary");
        assertFalse(isOnlyDictionaryEncodingPages(union(optional, notDictionary)), "optional notDictionary");
        assertFalse(isOnlyDictionaryEncodingPages(union(repeated, notDictionary)), "repeated notDictionary");
        assertFalse(isOnlyDictionaryEncodingPages(union(required, mixedDictionary)), "required mixedDictionary");
        assertFalse(isOnlyDictionaryEncodingPages(union(optional, mixedDictionary)), "optional mixedDictionary");
        assertFalse(isOnlyDictionaryEncodingPages(union(repeated, mixedDictionary)), "repeated mixedDictionary");
        assertTrue(isOnlyDictionaryEncodingPages(union(required, dictionary)), "required dictionary");
        assertTrue(isOnlyDictionaryEncodingPages(union(optional, dictionary)), "optional dictionary");
        assertTrue(isOnlyDictionaryEncodingPages(union(repeated, dictionary)), "repeated dictionary");
    }

    @Test
    public void testParquetTupleDomainPrimitiveArray()
    {
        HiveColumnHandle columnHandle = new HiveColumnHandle("my_array", HiveType.valueOf("array<int>"), parseTypeSignature(StandardTypes.ARRAY), 0, REGULAR, Optional.empty());
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
        HiveColumnHandle columnHandle = new HiveColumnHandle("my_array_struct", HiveType.valueOf("array<struct<a:int>>"), parseTypeSignature(StandardTypes.ARRAY), 0, REGULAR, Optional.empty());
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
        HiveColumnHandle columnHandle = new HiveColumnHandle("my_primitive", HiveType.valueOf("bigint"), parseTypeSignature(StandardTypes.BIGINT), 0, REGULAR, Optional.empty());
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
        HiveColumnHandle columnHandle = new HiveColumnHandle("my_struct", HiveType.valueOf("struct<a:int,b:int>"), parseTypeSignature(StandardTypes.ROW), 0, REGULAR, Optional.empty());
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
        HiveColumnHandle columnHandle = new HiveColumnHandle("my_map", HiveType.valueOf("map<int,int>"), parseTypeSignature(StandardTypes.MAP), 0, REGULAR, Optional.empty());

        MapType mapType = new MapType(
                INTEGER,
                INTEGER,
                methodHandle(TestParquetPredicateUtils.class, "throwUnsupportedOperationException"),
                methodHandle(TestParquetPredicateUtils.class, "throwUnsupportedOperationException"),
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
