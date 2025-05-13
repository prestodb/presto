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
package com.facebook.presto.block;

import com.facebook.presto.block.BlockAssertions.Encoding;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.block.BlockAssertions.Encoding.DICTIONARY;
import static com.facebook.presto.block.BlockAssertions.Encoding.RUN_LENGTH;
import static com.facebook.presto.block.BlockAssertions.assertBlockEquals;
import static com.facebook.presto.block.BlockAssertions.createAllNullsBlock;
import static com.facebook.presto.block.BlockAssertions.createLongSequenceBlock;
import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static com.facebook.presto.block.BlockAssertions.createMapBlock;
import static com.facebook.presto.block.BlockAssertions.createMapType;
import static com.facebook.presto.block.BlockAssertions.createRandomBlockForType;
import static com.facebook.presto.block.BlockAssertions.createRowBlock;
import static com.facebook.presto.block.BlockAssertions.createStringArraysBlock;
import static com.facebook.presto.block.BlockAssertions.createStringSequenceBlock;
import static com.facebook.presto.block.BlockAssertions.createStringsBlock;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.TestngUtils.toDataProvider;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestRowBasedSerialization
{
    private FunctionAndTypeManager functionAndTypeManager;

    @BeforeClass
    public void setUp()
    {
        Metadata metadata = MetadataManager.createTestMetadataManager();
        this.functionAndTypeManager = metadata.getFunctionAndTypeManager();
    }

    @AfterClass
    public void tearDown()
    {
        functionAndTypeManager = null;
    }

    @Test
    public void testFixedLength()
    {
        assertRoundTrip(ImmutableList.of(BIGINT), ImmutableList.of(createLongsBlock(emptyList())));
        assertRoundTrip(ImmutableList.of(BIGINT), ImmutableList.of(createLongsBlock(singletonList(null))));
        assertRoundTrip(ImmutableList.of(BIGINT), ImmutableList.of(createLongsBlock(asList(null, 1L, null, 3L))));
        assertRoundTrip(ImmutableList.of(BIGINT), ImmutableList.of(createLongsBlock(asList(0L, null, 1L, null, 3L))));
        assertRoundTrip(ImmutableList.of(BIGINT), ImmutableList.of(createLongSequenceBlock(-100, 100)));
    }

    @Test
    public void testVariableLength()
    {
        assertRoundTrip(ImmutableList.of(VARCHAR), ImmutableList.of(createStringsBlock()));
        assertRoundTrip(ImmutableList.of(VARCHAR), ImmutableList.of(createStringsBlock((String) null)));
        assertRoundTrip(ImmutableList.of(VARCHAR), ImmutableList.of(createStringsBlock((String) null, "abc")));
        assertRoundTrip(ImmutableList.of(VARCHAR), ImmutableList.of(createStringsBlock("bc", null, "abc")));
        assertRoundTrip(ImmutableList.of(VARCHAR), ImmutableList.of(createStringSequenceBlock(1, 100)));
    }

    @Test
    public void testArray()
    {
        assertRoundTrip(ImmutableList.of(new ArrayType(VARCHAR)), ImmutableList.of(createStringArraysBlock(emptyList())));
        assertRoundTrip(ImmutableList.of(new ArrayType(VARCHAR)), ImmutableList.of(createStringArraysBlock(singletonList(emptyList()))));
        assertRoundTrip(ImmutableList.of(new ArrayType(VARCHAR)), ImmutableList.of(createStringArraysBlock(singletonList((List<String>) null))));
        assertRoundTrip(ImmutableList.of(new ArrayType(VARCHAR)), ImmutableList.of(createStringArraysBlock(singletonList(singletonList(null)))));
        assertRoundTrip(ImmutableList.of(new ArrayType(VARCHAR)), ImmutableList.of(createStringArraysBlock(asList(null, singletonList(null)))));
        assertRoundTrip(ImmutableList.of(new ArrayType(VARCHAR)), ImmutableList.of(createStringArraysBlock(asList(null, asList("a", null)))));
        assertRoundTrip(ImmutableList.of(new ArrayType(VARCHAR)), ImmutableList.of(createStringArraysBlock(asList(asList(null, "b"), asList("a", null)))));
        assertRoundTrip(ImmutableList.of(new ArrayType(VARCHAR)), ImmutableList.of(createStringArraysBlock(asList(asList("b", "b"), asList("a", "c")))));
    }

    @Test
    public void testMap()
    {
        MapType mapType = createMapType(BIGINT, BIGINT);
        assertRoundTrip(ImmutableList.of(mapType), ImmutableList.of(createMapBlock(mapType, ImmutableMap.of(312L, 123L))));
        assertRoundTrip(ImmutableList.of(mapType), ImmutableList.of(createMapBlock(mapType, singletonMap(312L, null))));
        Map<Long, Long> map = new HashMap<>();
        map.put(1L, null);
        map.put(2L, 2L);
        map.put(3L, null);
        map.put(4L, 4L);
        assertRoundTrip(ImmutableList.of(mapType), ImmutableList.of(createMapBlock(mapType, map)));
    }

    @Test
    public void testRow()
    {
        Type rowType = functionAndTypeManager.getType(parseTypeSignature("row(x BIGINT, y VARCHAR)"));
        assertRoundTrip(ImmutableList.of(rowType), ImmutableList.of(createRowBlock(rowType.getTypeParameters())));
        assertRoundTrip(ImmutableList.of(rowType), ImmutableList.of(createRowBlock(rowType.getTypeParameters(), (Object[]) null)));
        assertRoundTrip(ImmutableList.of(rowType), ImmutableList.of(createRowBlock(rowType.getTypeParameters(), new Object[] {null, null})));
        assertRoundTrip(ImmutableList.of(rowType), ImmutableList.of(createRowBlock(rowType.getTypeParameters(), new Object[] {null, "string"})));
        assertRoundTrip(ImmutableList.of(rowType), ImmutableList.of(createRowBlock(rowType.getTypeParameters(), new Object[] {123L, "string"})));
        assertRoundTrip(ImmutableList.of(rowType), ImmutableList.of(createRowBlock(rowType.getTypeParameters(), new Object[] {123L, null})));
        assertRoundTrip(ImmutableList.of(rowType), ImmutableList.of(createRowBlock(rowType.getTypeParameters(),
                new Object[] {123L, null},
                new Object[] {123L, "string"},
                null,
                new Object[] {null, "string"},
                new Object[] {null, null},
                null)));
    }

    @DataProvider
    public Object[][] getTypeSignatures()
    {
        return ImmutableList.of(
                // all fixed length types
                "boolean",
                "smallint",
                "integer",
                "bigint",
                // variadic length type
                "varchar",
                // 8 bytes big decimal
                "decimal(2,1)",
                // 16 bytes big decimal
                "decimal(30,2)",
                // array of fixed length elements
                "array(bigint)",
                "array(decimal(30,2))",
                // array of variadic length elements
                "array(varchar)",
                // map of fixed length elements
                "map(bigint, decimal(30,2))",
                "map(decimal(30,2), bigint)",
                // map of variadic length elements
                "map(varchar, decimal(30,2))",
                "map(decimal(30,2), varchar)",
                "map(varchar, bigint)",
                "map(bigint, varchar)",
                "map(varchar, varchar)",
                // rows
                "row(x BIGINT)",
                "row(x VARCHAR)",
                "row(x BIGINT, y INTEGER)",
                "row(x BIGINT, y INTEGER, z VARCHAR)",
                "row(x VARCHAR, y INTEGER, z decimal(30,2))",
                // nested types
                "array(map(bigint, decimal(30,2)))",
                "map(array(varchar), bigint)",
                "map(array(varchar), array(array(varchar)))",
                "row(x array(varchar), y map(varchar, varchar), z row(x BIGINT))",
                "row(x array(varchar), y map(varchar, array(row(x BIGINT))), z row(x map(varchar, bigint)))"
        ).stream().collect(toDataProvider());
    }

    @Test(dataProvider = "getTypeSignatures")
    public void testAllNulls(String typeSignature)
    {
        Type type = functionAndTypeManager.getType(parseTypeSignature(typeSignature));
        List<Type> types = ImmutableList.of(type);
        assertRoundTrip(types, ImmutableList.of(createAllNullsBlock(type, 0)));
        assertRoundTrip(types, ImmutableList.of(createAllNullsBlock(type, 1)));
        assertRoundTrip(types, ImmutableList.of(createAllNullsBlock(type, 2)));
        assertRoundTrip(types, ImmutableList.of(createAllNullsBlock(type, 11)));
    }

    @Test(dataProvider = "getTypeSignatures")
    public void testRandom(String typeSignature)
    {
        Type type = functionAndTypeManager.getType(parseTypeSignature(typeSignature));
        testRandomBlocks(type, ImmutableList.of());
    }

    @Test(dataProvider = "getTypeSignatures")
    public void testRle(String typeSignature)
    {
        Type type = functionAndTypeManager.getType(parseTypeSignature(typeSignature));
        testRandomBlocks(type, ImmutableList.of(RUN_LENGTH));
        testRandomBlocks(type, ImmutableList.of(RUN_LENGTH, RUN_LENGTH));
        testRandomBlocks(type, ImmutableList.of(RUN_LENGTH, RUN_LENGTH, RUN_LENGTH));
    }

    @Test(dataProvider = "getTypeSignatures")
    public void testDictionary(String typeSignature)
    {
        Type type = functionAndTypeManager.getType(parseTypeSignature(typeSignature));
        testRandomBlocks(type, ImmutableList.of(DICTIONARY));
        testRandomBlocks(type, ImmutableList.of(DICTIONARY, DICTIONARY));
        testRandomBlocks(type, ImmutableList.of(DICTIONARY, DICTIONARY, DICTIONARY));
    }

    @Test(dataProvider = "getTypeSignatures")
    public void testMixedDictionaryAndRle(String typeSignature)
    {
        Type type = functionAndTypeManager.getType(parseTypeSignature(typeSignature));
        testRandomBlocks(type, ImmutableList.of(DICTIONARY, RUN_LENGTH));
        testRandomBlocks(type, ImmutableList.of(RUN_LENGTH, DICTIONARY));
        testRandomBlocks(type, ImmutableList.of(DICTIONARY, RUN_LENGTH, DICTIONARY));
        testRandomBlocks(type, ImmutableList.of(RUN_LENGTH, DICTIONARY, RUN_LENGTH, DICTIONARY, DICTIONARY));
    }

    private static void testRandomBlocks(Type type, List<Encoding> wrappings)
    {
        List<Type> types = ImmutableList.of(type);
        assertRoundTrip(types, ImmutableList.of(createRandomBlockForType(type, 10, 0, 0, false, wrappings)));
        assertRoundTrip(types, ImmutableList.of(createRandomBlockForType(type, 11, 0, 0, false, wrappings)));
        assertRoundTrip(types, ImmutableList.of(createRandomBlockForType(type, 100, 0.5f, 0, false, wrappings)));
        assertRoundTrip(types, ImmutableList.of(createRandomBlockForType(type, 33, 0, 0, false, wrappings)));
        assertRoundTrip(types, ImmutableList.of(createRandomBlockForType(type, 27, 0.5f, 0, false, wrappings)));
        assertRoundTrip(types, ImmutableList.of(createRandomBlockForType(type, 100, 0, 0.5f, false, wrappings)));
        assertRoundTrip(types, ImmutableList.of(createRandomBlockForType(type, 100, 0.5f, 0.5f, false, wrappings)));
    }

    private static void assertRoundTrip(List<Type> types, List<Block> blocks)
    {
        assertEquals(types.size(), blocks.size());
        List<Slice> rows = serialize(blocks);
        List<Block> deserialized = deserialize(types, rows);
        assertEquals(blocks.size(), deserialized.size());
        for (int i = 0; i < types.size(); i++) {
            assertBlockEquals(types.get(i), deserialized.get(i), blocks.get(i));
        }
    }

    private static List<Slice> serialize(List<Block> blocks)
    {
        if (blocks.isEmpty()) {
            return ImmutableList.of();
        }
        int positions = blocks.get(0).getPositionCount();
        for (Block block : blocks) {
            assertEquals(block.getPositionCount(), positions);
        }
        ImmutableList.Builder<Slice> result = ImmutableList.builder();
        for (int position = 0; position < positions; position++) {
            result.add(serializePosition(position, blocks));
        }
        return result.build();
    }

    private static List<Block> deserialize(List<Type> types, List<Slice> rows)
    {
        if (types.isEmpty()) {
            assertTrue(rows.isEmpty());
            return ImmutableList.of();
        }

        List<BlockBuilder> blockBuilders = types.stream()
                .map(type -> type.createBlockBuilder(null, rows.size()))
                .collect(toImmutableList());

        for (Slice row : rows) {
            appendRow(row, blockBuilders);
        }

        return blockBuilders.stream()
                .map(BlockBuilder::build)
                .collect(toImmutableList());
    }

    private static Slice serializePosition(int position, List<Block> blocks)
    {
        try (DynamicSliceOutput output = new DynamicSliceOutput(256)) {
            for (Block block : blocks) {
                block.writePositionTo(position, output);
            }
            output.close();
            return output.slice();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void appendRow(Slice row, List<BlockBuilder> blockBuilders)
    {
        try (SliceInput input = new BasicSliceInput(row)) {
            for (BlockBuilder blockBuilder : blockBuilders) {
                blockBuilder.readPositionFrom(input);
            }
        }
    }
}
