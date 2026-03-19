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
package com.facebook.presto.lance;

import com.facebook.plugin.arrow.ArrowBlockBuilder;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.spi.ColumnHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import org.lance.Fragment;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URL;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 * Tests reading from wide_types_table.lance which contains many Arrow types
 * including Float16 columns. Verifies the full read path through
 * LanceFragmentPageSource including Float16-to-Float32 widening.
 *
 * Dataset schema (2 rows):
 *   id           int64           -> BIGINT        [1, 2]
 *   col_bool     bool            -> BOOLEAN       [true, false]
 *   col_int32    int32           -> INTEGER       [10, -10]
 *   col_int64    int64           -> BIGINT        [100, -100]
 *   col_uint64   uint64          -> BIGINT        [42, 99]
 *   col_float16  float16         -> REAL          [3.5, -3.5]
 *   col_float32  float32         -> REAL          [1.5, -1.5]
 *   col_float64  float64         -> DOUBLE        [2.5, -2.5]
 *   col_string   utf8            -> VARCHAR       ["hello", "world"]
 *   col_binary   binary          -> VARBINARY     [[0x01,0x02], [0x03,0x04]]
 *   col_date     date32          -> DATE          [2024-01-15, 2024-06-30]
 *   col_ts       timestamp[us]   -> TIMESTAMP
 *   col_ts_tz    timestamp[us,UTC] -> TIMESTAMP
 *   col_list_f32 list(float32)   -> ARRAY(REAL)   [[1.0,2.0], [3.0,4.0,5.0]]
 *   col_fsl_f32  fsl(float32)[3] -> ARRAY(REAL)   [[1.0,2.0,3.0], [4.0,5.0,6.0]]
 *   col_fsl_f16  fsl(float16)[3] -> ARRAY(REAL)   [[7.0,8.0,9.0], [10.0,11.0,12.0]]
 */
@Test(singleThreaded = true)
public class TestWideTypesTable
{
    private LanceNamespaceHolder namespaceHolder;
    private LanceTableHandle tableHandle;
    private String tablePath;
    private List<Fragment> fragments;
    private ArrowBlockBuilder arrowBlockBuilder;
    private Map<String, ColumnHandle> columnHandles;

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        URL dbUrl = Resources.getResource(TestWideTypesTable.class, "/example_db");
        assertNotNull(dbUrl, "example_db resource not found");
        String rootPath = Paths.get(dbUrl.toURI()).toString();
        LanceConfig config = new LanceConfig()
                .setRootUrl(rootPath)
                .setSingleLevelNs(true);
        namespaceHolder = new LanceNamespaceHolder(config);
        arrowBlockBuilder = new ArrowBlockBuilder(createTestFunctionAndTypeManager());
        tableHandle = new LanceTableHandle("default", "wide_types_table");
        tablePath = namespaceHolder.getTablePath("wide_types_table");
        fragments = namespaceHolder.getFragments("wide_types_table");
        LanceMetadata metadata = new LanceMetadata(namespaceHolder, jsonCodec(LanceCommitTaskData.class));
        columnHandles = metadata.getColumnHandles(null, tableHandle);
    }

    @Test
    public void testReadAllColumns()
            throws Exception
    {
        // Project all columns (uint64 and nested float16 are now coerced)
        List<LanceColumnHandle> columns = columnHandles.values().stream()
                .map(LanceColumnHandle.class::cast)
                .collect(toImmutableList());

        Page page = readColumns(columns);
        assertEquals(page.getPositionCount(), 2);
        assertTrue(page.getChannelCount() > 0);
    }

    @Test
    public void testFloat16Column()
    {
        LanceColumnHandle col = (LanceColumnHandle) columnHandles.get("col_float16");
        assertNotNull(col, "col_float16 not found in schema");
        assertEquals(col.getColumnType(), REAL);

        Page page = readColumns(ImmutableList.of(col));
        assertEquals(page.getPositionCount(), 2);

        Block block = page.getBlock(0);
        float val0 = Float.intBitsToFloat((int) REAL.getLong(block, 0));
        float val1 = Float.intBitsToFloat((int) REAL.getLong(block, 1));
        assertEquals(val0, 3.5f, 0.01f);
        assertEquals(val1, -3.5f, 0.01f);
    }

    @Test
    public void testFloat32Column()
    {
        LanceColumnHandle col = (LanceColumnHandle) columnHandles.get("col_float32");
        Page page = readColumns(ImmutableList.of(col));

        Block block = page.getBlock(0);
        float val0 = Float.intBitsToFloat((int) REAL.getLong(block, 0));
        float val1 = Float.intBitsToFloat((int) REAL.getLong(block, 1));
        assertEquals(val0, 1.5f, 0.01f);
        assertEquals(val1, -1.5f, 0.01f);
    }

    @Test
    public void testFloat64Column()
    {
        LanceColumnHandle col = (LanceColumnHandle) columnHandles.get("col_float64");
        Page page = readColumns(ImmutableList.of(col));

        Block block = page.getBlock(0);
        assertEquals(DOUBLE.getDouble(block, 0), 2.5, 0.01);
        assertEquals(DOUBLE.getDouble(block, 1), -2.5, 0.01);
    }

    @Test
    public void testIntegerColumns()
    {
        LanceColumnHandle colId = (LanceColumnHandle) columnHandles.get("id");
        LanceColumnHandle colInt32 = (LanceColumnHandle) columnHandles.get("col_int32");
        LanceColumnHandle colInt64 = (LanceColumnHandle) columnHandles.get("col_int64");
        Page page = readColumns(ImmutableList.of(colId, colInt32, colInt64));

        // id
        assertEquals(BIGINT.getLong(page.getBlock(0), 0), 1L);
        assertEquals(BIGINT.getLong(page.getBlock(0), 1), 2L);
        // col_int32
        assertEquals(INTEGER.getLong(page.getBlock(1), 0), 10L);
        assertEquals(INTEGER.getLong(page.getBlock(1), 1), -10L);
        // col_int64
        assertEquals(BIGINT.getLong(page.getBlock(2), 0), 100L);
        assertEquals(BIGINT.getLong(page.getBlock(2), 1), -100L);
    }

    @Test
    public void testBooleanColumn()
    {
        LanceColumnHandle col = (LanceColumnHandle) columnHandles.get("col_bool");
        Page page = readColumns(ImmutableList.of(col));

        assertTrue(BOOLEAN.getBoolean(page.getBlock(0), 0));
        assertFalse(BOOLEAN.getBoolean(page.getBlock(0), 1));
    }

    @Test
    public void testVarcharColumn()
    {
        LanceColumnHandle col = (LanceColumnHandle) columnHandles.get("col_string");
        Page page = readColumns(ImmutableList.of(col));

        assertEquals(VARCHAR.getSlice(page.getBlock(0), 0).toStringUtf8(), "hello");
        assertEquals(VARCHAR.getSlice(page.getBlock(0), 1).toStringUtf8(), "world");
    }

    @Test
    public void testDateColumn()
    {
        LanceColumnHandle col = (LanceColumnHandle) columnHandles.get("col_date");
        Page page = readColumns(ImmutableList.of(col));

        // 2024-01-15 = 19737 days since epoch
        assertEquals(DATE.getLong(page.getBlock(0), 0), 19737L);
        // 2024-06-30 = 19904 days since epoch
        assertEquals(DATE.getLong(page.getBlock(0), 1), 19904L);
    }

    @Test
    public void testFixedSizeListFloat32()
    {
        LanceColumnHandle col = (LanceColumnHandle) columnHandles.get("col_fsl_f32");
        assertNotNull(col, "col_fsl_f32 not found");
        Page page = readColumns(ImmutableList.of(col));

        ArrayType arrayType = (ArrayType) col.getColumnType();
        Block inner0 = (Block) arrayType.getObject(page.getBlock(0), 0);
        assertEquals(inner0.getPositionCount(), 3);
        assertEquals(Float.intBitsToFloat((int) REAL.getLong(inner0, 0)), 1.0f, 0.01f);
        assertEquals(Float.intBitsToFloat((int) REAL.getLong(inner0, 1)), 2.0f, 0.01f);
        assertEquals(Float.intBitsToFloat((int) REAL.getLong(inner0, 2)), 3.0f, 0.01f);
    }

    @Test
    public void testUint64Column()
    {
        LanceColumnHandle col = (LanceColumnHandle) columnHandles.get("col_uint64");
        assertNotNull(col, "col_uint64 not found in schema");
        assertEquals(col.getColumnType(), BIGINT);

        Page page = readColumns(ImmutableList.of(col));
        assertEquals(BIGINT.getLong(page.getBlock(0), 0), 42L);
        assertEquals(BIGINT.getLong(page.getBlock(0), 1), 99L);
    }

    @Test
    public void testFixedSizeListFloat16()
    {
        LanceColumnHandle col = (LanceColumnHandle) columnHandles.get("col_fsl_f16");
        assertNotNull(col, "col_fsl_f16 not found");
        Page page = readColumns(ImmutableList.of(col));

        ArrayType arrayType = (ArrayType) col.getColumnType();
        Block inner0 = (Block) arrayType.getObject(page.getBlock(0), 0);
        assertEquals(inner0.getPositionCount(), 3);
        assertEquals(Float.intBitsToFloat((int) REAL.getLong(inner0, 0)), 7.0f, 0.01f);
        assertEquals(Float.intBitsToFloat((int) REAL.getLong(inner0, 1)), 8.0f, 0.01f);
        assertEquals(Float.intBitsToFloat((int) REAL.getLong(inner0, 2)), 9.0f, 0.01f);

        Block inner1 = (Block) arrayType.getObject(page.getBlock(0), 1);
        assertEquals(inner1.getPositionCount(), 3);
        assertEquals(Float.intBitsToFloat((int) REAL.getLong(inner1, 0)), 10.0f, 0.01f);
        assertEquals(Float.intBitsToFloat((int) REAL.getLong(inner1, 1)), 11.0f, 0.01f);
        assertEquals(Float.intBitsToFloat((int) REAL.getLong(inner1, 2)), 12.0f, 0.01f);
    }

    private Page readColumns(List<LanceColumnHandle> columns)
    {
        try {
            LanceFragmentPageSource pageSource = new LanceFragmentPageSource(
                    tableHandle,
                    columns,
                    ImmutableList.of(fragments.get(0).getId()),
                    tablePath,
                    8192,
                    arrowBlockBuilder,
                    namespaceHolder.getAllocator());
            try {
                Page page = pageSource.getNextPage();
                assertNotNull(page);
                return page;
            }
            finally {
                pageSource.close();
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
