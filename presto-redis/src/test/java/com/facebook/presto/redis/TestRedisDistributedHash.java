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
package com.facebook.presto.redis;

import com.facebook.presto.redis.util.EmbeddedRedis;
import com.facebook.presto.tests.AbstractTestDistributedQueries;
import io.airlift.tpch.TpchTable;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.IOException;

import static com.facebook.presto.redis.util.EmbeddedRedis.createEmbeddedRedis;
import static io.airlift.testing.Closeables.closeAllRuntimeException;

@Test
public class TestRedisDistributedHash
        extends AbstractTestDistributedQueries
{
    private final EmbeddedRedis embeddedRedis;

    public TestRedisDistributedHash()
            throws Exception
    {
        this(createEmbeddedRedis());
    }

    public TestRedisDistributedHash(EmbeddedRedis embeddedRedis)
            throws Exception
    {
        super(RedisQueryRunner.createRedisQueryRunner(embeddedRedis, "hash", TpchTable.getTables()));
        this.embeddedRedis = embeddedRedis;
    }

    @AfterClass(alwaysRun = true)
    public void destroy()
            throws IOException
    {
        closeAllRuntimeException(queryRunner, embeddedRedis);
    }

    //
    // Redis connector does not support table creation.
    //

    @Override
    public void testCreateTable()
    {
    }

    @Override
    public void testCreateTableAsSelect()
    {
    }

    @Override
    public void testSymbolAliasing()
    {
    }

    //
    // Redis connector does not support views.
    //

    @Override
    public void testView()
    {
    }

    @Override
    public void testCompatibleTypeChangeForView()
            throws Exception
    {
        // Redis connector currently does not support views
    }

    @Override
    public void testCompatibleTypeChangeForView2()
            throws Exception
    {
        // Redis connector currently does not support views
    }

    @Override
    public void testViewMetadata()
    {
    }

    @Test
    public void testViewCaseSensitivity()
            throws Exception
    {
        // Redis connector currently does not support views
    }

    //
    // Redis connector does not insert.
    //

    @Override
    public void testInsert()
    {
    }

    //
    // Redis connector does not delete.
    //

    @Override
    public void testDelete()
    {
    }

    //
    // Redis connector does not table rename.
    //

    @Override
    public void testRenameTable()
    {
    }

    //
    // Redis connector does not add/rename table column.
    //

    @Override
    public void testAddColumn()
    {
    }

    @Override
    public void testRenameColumn()
    {
    }

    //
    // Redis connector does not drop table.
    //

    @Override
    public void testDropTableIfExists()
    {
    }

    //
    // Redis doesn't support date type yet
    //

    @Override
    public void testShowColumns()
    {
    }

    @Override
    public void testGroupingSetMixedExpressionAndColumn()
            throws Exception
    {
    }

    @Override
    public void testGroupingSetMixedExpressionAndOrdinal()
            throws Exception
    {
    }

    //
    // Redis connector always return a single split for the test data set
    // TODO fix test to have multiple splits
    //
    @Override
    public void testTableSampleSystem()
    {
    }
}
