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

import com.facebook.presto.tests.AbstractTestDistributedQueries;
import io.airlift.tpch.TpchTable;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import com.facebook.presto.redis.util.EmbeddedRedis;
import static com.facebook.presto.redis.util.EmbeddedRedis.createEmbeddedRedis;
import static io.airlift.testing.Closeables.closeAllRuntimeException;
import java.io.IOException;

@Test
public class TestRedisDistributed
        extends AbstractTestDistributedQueries
{
    private final EmbeddedRedis embeddedRedis;

    public TestRedisDistributed()
            throws Exception
    {
        this(createEmbeddedRedis());
    }

    public TestRedisDistributed(EmbeddedRedis embeddedRedis)
            throws Exception
    {
        super(RedisQueryRunner.createRedisQueryRunner(embeddedRedis, "string", TpchTable.getTables()));
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
    public void testCreateSampledTableAsSelectLimit()
            throws Exception
    {
    }

    @Override
    public void testCreateTableAsSelect()
            throws Exception
    {
    }

    @Override
    public void testCreateTableAsSelectGroupBy()
            throws Exception
    {
    }

    @Override
    public void testCreateTableAsSelectJoin()
            throws Exception
    {
    }

    @Override
    public void testCreateTableAsSelectLimit()
            throws Exception
    {
    }

    @Override
    public void testSymbolAliasing()
            throws Exception
    {
    }

    //
    // Redis connector does not support views.
    //

    @Override
    public void testView()
            throws Exception
    {
    }

    @Override
    public void testViewMetadata()
            throws Exception
    {
    }

    //
    // Redis connector does not insert.
    //

    @Override
    public void testInsert()
            throws Exception
    {
    }

    //
    // Redis connector does not table rename.
    //

    @Override
    public void testRenameTable()
            throws Exception
    {
    }

    //
    // Redis connector creates one split only that's not compatible with the Tablesample
    //

    @Override
    public void testTableSampleSystem()
            throws Exception
    {
    }

    //
    // Misc excludes for column type mismatch for Redis connector
    //

    // incompatible types: varchar, bigint

    @Override
    public void testLimitPushDown()
            throws Exception
    {
    }

    // incompatible types: varchar, bigint
    @Override
    public void testTableQueryInUnion()
            throws Exception
    {
    }

    // expected [[orderdate, date, true, false, ]] but found [[orderdate, varchar, true, false, ]]
    @Override
    public void testShowColumns()
            throws Exception
    {
    }

    // Unexpected parameters (varchar) for function day. Expected: day(timestamp)
    @Override
    public void testUnionWithProjectionPushDown()
            throws Exception
    {
    }

    // drop is not supported
    @Override
    public void testDropTableIfExists()
            throws Exception
    {
    }

    // create table is not supported
    @Override
    public void testCreateTable()
            throws Exception
    {
    }

    // column renaming is not supported
    @Override
    public void testRenameColumn()
            throws Exception
    {
    }
}
