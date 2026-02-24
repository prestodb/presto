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
package com.facebook.presto.client;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.MethodHandleUtil;
import com.facebook.presto.common.block.TestingBlockEncodingSerde;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.page.PagesSerde;
import com.facebook.presto.spi.page.SerializedPage;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Base64;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.page.PagesSerdeUtil.writeSerializedPage;
import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

@Test(singleThreaded = true)
public class TestBinaryDataDeserializer
{
    private TestingBlockEncodingSerde blockEncodingSerde;
    private ClientTypeManager typeManager;
    private PagesSerde pagesSerde;
    private BinaryDataDeserializer deserializer;
    private ClientSession testSession;

    @BeforeMethod
    public void setUp()
    {
        blockEncodingSerde = new TestingBlockEncodingSerde();
        typeManager = new ClientTypeManager();
        pagesSerde = new PagesSerde(blockEncodingSerde, Optional.empty(), Optional.empty(), Optional.empty(), false);
        testSession = createTestClientSession();
        deserializer = new BinaryDataDeserializer(blockEncodingSerde, typeManager, testSession);
    }

    private ClientSession createTestClientSession()
    {
        return new ClientSession(
                URI.create("http://localhost:8080"),
                "test-user",
                "test-source",
                Optional.empty(),
                ImmutableSet.of(),
                null,
                null,
                null,
                "America/Los_Angeles",
                java.util.Locale.US,
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                null,
                com.facebook.airlift.units.Duration.valueOf("2m"),
                false,
                ImmutableMap.of(),
                ImmutableMap.of(),
                false,
                false);
    }

    @Test
    public void testDeserializePrimitiveTypes()
    {
        VarcharType varcharType = VarcharType.createVarcharType(100);
        List<Column> columns = ImmutableList.of(
                new Column("col1", BigintType.BIGINT),
                new Column("col2", varcharType),
                new Column("col3", BooleanType.BOOLEAN),
                new Column("col4", IntegerType.INTEGER),
                new Column("col5", DoubleType.DOUBLE));

        BlockBuilder bigintBlockBuilder = BigintType.BIGINT.createBlockBuilder(null, 3);
        bigintBlockBuilder.writeLong(123L);
        bigintBlockBuilder.writeLong(456L);
        bigintBlockBuilder.writeLong(789L);

        BlockBuilder varcharBlockBuilder = varcharType.createBlockBuilder(null, 3);
        varcharType.writeSlice(varcharBlockBuilder, utf8Slice("hello"));
        varcharType.writeSlice(varcharBlockBuilder, utf8Slice("world"));
        varcharType.writeSlice(varcharBlockBuilder, utf8Slice("test"));

        BlockBuilder booleanBlockBuilder = BooleanType.BOOLEAN.createBlockBuilder(null, 3);
        BooleanType.BOOLEAN.writeBoolean(booleanBlockBuilder, true);
        BooleanType.BOOLEAN.writeBoolean(booleanBlockBuilder, false);
        BooleanType.BOOLEAN.writeBoolean(booleanBlockBuilder, true);

        BlockBuilder integerBlockBuilder = IntegerType.INTEGER.createBlockBuilder(null, 3);
        IntegerType.INTEGER.writeLong(integerBlockBuilder, 10);
        IntegerType.INTEGER.writeLong(integerBlockBuilder, 20);
        IntegerType.INTEGER.writeLong(integerBlockBuilder, 30);

        BlockBuilder doubleBlockBuilder = DoubleType.DOUBLE.createBlockBuilder(null, 3);
        DoubleType.DOUBLE.writeDouble(doubleBlockBuilder, 1.5);
        DoubleType.DOUBLE.writeDouble(doubleBlockBuilder, 2.5);
        DoubleType.DOUBLE.writeDouble(doubleBlockBuilder, 3.5);

        Page page = new Page(
                bigintBlockBuilder.build(),
                varcharBlockBuilder.build(),
                booleanBlockBuilder.build(),
                integerBlockBuilder.build(),
                doubleBlockBuilder.build());

        String encodedPage = serializePage(page);

        Iterable<List<Object>> rows = deserializer.deserialize(columns, ImmutableList.of(encodedPage));

        List<List<Object>> rowList = ImmutableList.copyOf(rows);
        assertEquals(rowList.size(), 3);

        assertEquals(rowList.get(0).get(0), 123L);
        assertEquals(rowList.get(0).get(1), "hello");
        assertEquals(rowList.get(0).get(2), true);
        assertEquals(rowList.get(0).get(3), 10);
        assertEquals(rowList.get(0).get(4), 1.5);

        assertEquals(rowList.get(1).get(0), 456L);
        assertEquals(rowList.get(1).get(1), "world");
        assertEquals(rowList.get(1).get(2), false);
        assertEquals(rowList.get(1).get(3), 20);
        assertEquals(rowList.get(1).get(4), 2.5);

        assertEquals(rowList.get(2).get(0), 789L);
        assertEquals(rowList.get(2).get(1), "test");
        assertEquals(rowList.get(2).get(2), true);
        assertEquals(rowList.get(2).get(3), 30);
        assertEquals(rowList.get(2).get(4), 3.5);
    }

    @Test
    public void testDeserializeNullValues()
    {
        VarcharType varcharType = VarcharType.createVarcharType(100);
        List<Column> columns = ImmutableList.of(
                new Column("col1", BigintType.BIGINT),
                new Column("col2", varcharType));

        BlockBuilder bigintBlockBuilder = BigintType.BIGINT.createBlockBuilder(null, 2);
        bigintBlockBuilder.writeLong(100L);
        bigintBlockBuilder.appendNull();

        BlockBuilder varcharBlockBuilder = varcharType.createBlockBuilder(null, 2);
        varcharBlockBuilder.appendNull();
        varcharType.writeSlice(varcharBlockBuilder, utf8Slice("text"));

        Page page = new Page(bigintBlockBuilder.build(), varcharBlockBuilder.build());
        String encodedPage = serializePage(page);

        Iterable<List<Object>> rows = deserializer.deserialize(columns, ImmutableList.of(encodedPage));

        List<List<Object>> rowList = ImmutableList.copyOf(rows);
        assertEquals(rowList.size(), 2);

        assertEquals(rowList.get(0).get(0), 100L);
        assertNull(rowList.get(0).get(1));

        assertNull(rowList.get(1).get(0));
        assertEquals(rowList.get(1).get(1), "text");
    }

    @Test
    public void testDeserializeArrayType()
    {
        ArrayType arrayType = new ArrayType(BigintType.BIGINT);

        List<Column> columns = ImmutableList.of(
                new Column("arr", arrayType));

        BlockBuilder arrayBlockBuilder = arrayType.createBlockBuilder(null, 2);

        BlockBuilder arrayElementBuilder1 = arrayBlockBuilder.beginBlockEntry();
        BigintType.BIGINT.writeLong(arrayElementBuilder1, 1L);
        BigintType.BIGINT.writeLong(arrayElementBuilder1, 2L);
        BigintType.BIGINT.writeLong(arrayElementBuilder1, 3L);
        arrayBlockBuilder.closeEntry();

        BlockBuilder arrayElementBuilder2 = arrayBlockBuilder.beginBlockEntry();
        BigintType.BIGINT.writeLong(arrayElementBuilder2, 4L);
        BigintType.BIGINT.writeLong(arrayElementBuilder2, 5L);
        arrayBlockBuilder.closeEntry();

        Page page = new Page(arrayBlockBuilder.build());
        String encodedPage = serializePage(page);

        Iterable<List<Object>> rows = deserializer.deserialize(columns, ImmutableList.of(encodedPage));

        List<List<Object>> rowList = ImmutableList.copyOf(rows);
        assertEquals(rowList.size(), 2);

        assertNotNull(rowList.get(0).get(0));
        assertNotNull(rowList.get(1).get(0));
    }

    @Test
    public void testDeserializeMapType()
    {
        VarcharType varcharType = VarcharType.createVarcharType(100);
        MapType mapType = new MapType(
                varcharType,
                BigintType.BIGINT,
                MethodHandleUtil.methodHandle(TestBinaryDataDeserializer.class, "blockEquals", Block.class, Block.class),
                MethodHandleUtil.methodHandle(TestBinaryDataDeserializer.class, "blockHashCode", Block.class));

        List<Column> columns = ImmutableList.of(
                new Column("map", mapType));

        BlockBuilder mapBlockBuilder = mapType.createBlockBuilder(null, 1);
        BlockBuilder singleMapWriter = mapBlockBuilder.beginBlockEntry();
        varcharType.writeSlice(singleMapWriter, utf8Slice("key1"));
        BigintType.BIGINT.writeLong(singleMapWriter, 100L);
        varcharType.writeSlice(singleMapWriter, utf8Slice("key2"));
        BigintType.BIGINT.writeLong(singleMapWriter, 200L);
        mapBlockBuilder.closeEntry();

        Page page = new Page(mapBlockBuilder.build());
        String encodedPage = serializePage(page);

        Iterable<List<Object>> rows = deserializer.deserialize(columns, ImmutableList.of(encodedPage));

        List<List<Object>> rowList = ImmutableList.copyOf(rows);
        assertEquals(rowList.size(), 1);
        assertNotNull(rowList.get(0).get(0));
    }

    @Test
    public void testDeserializeRowType()
    {
        VarcharType varcharType = VarcharType.createVarcharType(100);
        RowType rowType = RowType.from(ImmutableList.of(
                new RowType.Field(Optional.of("field1"), BigintType.BIGINT, false),
                new RowType.Field(Optional.of("field2"), varcharType, false)));

        List<Column> columns = ImmutableList.of(
                new Column("row", rowType));

        BlockBuilder rowBlockBuilder = rowType.createBlockBuilder(null, 1);
        BlockBuilder singleRowBuilder = rowBlockBuilder.beginBlockEntry();
        BigintType.BIGINT.writeLong(singleRowBuilder, 42L);
        varcharType.writeSlice(singleRowBuilder, utf8Slice("test"));
        rowBlockBuilder.closeEntry();

        Page page = new Page(rowBlockBuilder.build());
        String encodedPage = serializePage(page);

        Iterable<List<Object>> rows = deserializer.deserialize(columns, ImmutableList.of(encodedPage));

        List<List<Object>> rowList = ImmutableList.copyOf(rows);
        assertEquals(rowList.size(), 1);
        assertNotNull(rowList.get(0).get(0));
    }

    @Test
    public void testDeserializeMultiplePages()
    {
        List<Column> columns = ImmutableList.of(
                new Column("col1", BigintType.BIGINT));

        BlockBuilder builder1 = BigintType.BIGINT.createBlockBuilder(null, 2);
        builder1.writeLong(1L);
        builder1.writeLong(2L);
        Page page1 = new Page(builder1.build());

        BlockBuilder builder2 = BigintType.BIGINT.createBlockBuilder(null, 2);
        builder2.writeLong(3L);
        builder2.writeLong(4L);
        Page page2 = new Page(builder2.build());

        String encodedPage1 = serializePage(page1);
        String encodedPage2 = serializePage(page2);

        Iterable<List<Object>> rows = deserializer.deserialize(
                columns,
                ImmutableList.of(encodedPage1, encodedPage2));

        List<List<Object>> rowList = ImmutableList.copyOf(rows);
        assertEquals(rowList.size(), 4);
        assertEquals(rowList.get(0).get(0), 1L);
        assertEquals(rowList.get(1).get(0), 2L);
        assertEquals(rowList.get(2).get(0), 3L);
        assertEquals(rowList.get(3).get(0), 4L);
    }

    @Test
    public void testDeserializeEmptyPage()
    {
        List<Column> columns = ImmutableList.of(
                new Column("col1", BigintType.BIGINT));

        BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, 0);
        Page page = new Page(builder.build());

        String encodedPage = serializePage(page);

        Iterable<List<Object>> rows = deserializer.deserialize(columns, ImmutableList.of(encodedPage));

        List<List<Object>> rowList = ImmutableList.copyOf(rows);
        assertEquals(rowList.size(), 0);
    }

    public static boolean blockEquals(Block left, Block right)
    {
        return left.equals(right);
    }

    public static long blockHashCode(Block block)
    {
        return block.hashCode();
    }

    private String serializePage(Page page)
    {
        SerializedPage serializedPage = pagesSerde.serialize(page);

        DynamicSliceOutput output = new DynamicSliceOutput(serializedPage.getSizeInBytes());
        writeSerializedPage(output, serializedPage);

        Slice slice = output.slice();
        byte[] bytes = slice.getBytes();

        return Base64.getEncoder().encodeToString(bytes);
    }
}
