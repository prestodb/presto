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

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
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
import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestLanceFragmentPageSource
{
    private LanceNamespaceHolder namespaceHolder;
    private LanceTableHandle tableHandle;
    private String tablePath;
    private List<Fragment> fragments;

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        URL dbUrl = Resources.getResource(TestLanceFragmentPageSource.class, "/example_db");
        assertNotNull(dbUrl, "example_db resource not found");
        String rootPath = Paths.get(dbUrl.toURI()).toString();
        LanceConfig config = new LanceConfig()
                .setRootUrl(rootPath)
                .setSingleLevelNs(true);
        namespaceHolder = new LanceNamespaceHolder(config);
        tableHandle = new LanceTableHandle("default", "test_table1");
        tablePath = namespaceHolder.getTablePath("default", "test_table1");
        fragments = namespaceHolder.getFragments("default", "test_table1");
    }

    @Test
    public void testFragmentScan()
            throws Exception
    {
        List<LanceColumnHandle> columns = getColumns();

        try (LanceFragmentPageSource pageSource = new LanceFragmentPageSource(
                tableHandle,
                columns,
                ImmutableList.of(fragments.get(0).getId()),
                tablePath,
                8192)) {
            Page page = pageSource.getNextPage();
            assertNotNull(page);
            assertEquals(page.getChannelCount(), 4);
            assertEquals(page.getPositionCount(), 2);

            // Verify first column (x) has expected values
            Block xBlock = page.getBlock(0);
            assertEquals(BIGINT.getLong(xBlock, 0), 0L);

            Page nextPage = pageSource.getNextPage();
            assertNull(nextPage);
            assertTrue(pageSource.isFinished());
        }
    }

    @Test
    public void testColumnProjection()
            throws Exception
    {
        Map<String, ColumnHandle> columnHandleMap = getColumnHandles();
        LanceColumnHandle colB = (LanceColumnHandle) columnHandleMap.get("b");
        LanceColumnHandle colX = (LanceColumnHandle) columnHandleMap.get("x");
        List<LanceColumnHandle> projectedColumns = ImmutableList.of(colB, colX);

        try (LanceFragmentPageSource pageSource = new LanceFragmentPageSource(
                tableHandle,
                projectedColumns,
                ImmutableList.of(fragments.get(0).getId()),
                tablePath,
                8192)) {
            Page page = pageSource.getNextPage();
            assertNotNull(page);
            assertEquals(page.getChannelCount(), 2);
            assertEquals(page.getPositionCount(), 2);

            Block bBlock = page.getBlock(0);
            assertEquals(BIGINT.getLong(bBlock, 0), 0L);
            assertEquals(BIGINT.getLong(bBlock, 1), 3L);

            Block xBlock = page.getBlock(1);
            assertEquals(BIGINT.getLong(xBlock, 0), 0L);
            assertEquals(BIGINT.getLong(xBlock, 1), 1L);
        }
    }

    @Test
    public void testPartialColumnProjection()
            throws Exception
    {
        Map<String, ColumnHandle> columnHandleMap = getColumnHandles();
        LanceColumnHandle colC = (LanceColumnHandle) columnHandleMap.get("c");
        LanceColumnHandle colX = (LanceColumnHandle) columnHandleMap.get("x");
        List<LanceColumnHandle> projectedColumns = ImmutableList.of(colC, colX);

        try (LanceFragmentPageSource pageSource = new LanceFragmentPageSource(
                tableHandle,
                projectedColumns,
                ImmutableList.of(fragments.get(0).getId()),
                tablePath,
                8192)) {
            Page page = pageSource.getNextPage();
            assertNotNull(page);
            assertEquals(page.getChannelCount(), 2);
            assertEquals(page.getPositionCount(), 2);

            Block cBlock = page.getBlock(0);
            assertEquals(BIGINT.getLong(cBlock, 0), 0L);
            assertEquals(BIGINT.getLong(cBlock, 1), -1L);

            Block xBlock = page.getBlock(1);
            assertEquals(BIGINT.getLong(xBlock, 0), 0L);
            assertEquals(BIGINT.getLong(xBlock, 1), 1L);
        }
    }

    private List<LanceColumnHandle> getColumns()
    {
        return getColumnHandles().values().stream()
                .map(LanceColumnHandle.class::cast)
                .collect(toImmutableList());
    }

    private Map<String, ColumnHandle> getColumnHandles()
    {
        LanceMetadata metadata = new LanceMetadata(namespaceHolder, jsonCodec(LanceCommitTaskData.class));
        return metadata.getColumnHandles(null, tableHandle);
    }
}
