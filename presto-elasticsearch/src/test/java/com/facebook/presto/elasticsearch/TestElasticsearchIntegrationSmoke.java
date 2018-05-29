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
package com.facebook.presto.elasticsearch;

import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestIntegrationSmokeTest;
import com.google.common.io.Closer;
import io.airlift.tpch.TpchTable;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.UncheckedIOException;

import static com.facebook.presto.elasticsearch.EmbeddedElasticsearchNode.createEmbeddedElasticsearch;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;

public class TestElasticsearchIntegrationSmoke
        extends AbstractTestIntegrationSmokeTest
{
    private final EmbeddedElasticsearchNode embeddedElasticsearch;

    private QueryRunner queryRunner;

    public TestElasticsearchIntegrationSmoke()
            throws Exception
    {
        this(createEmbeddedElasticsearch());
    }

    public TestElasticsearchIntegrationSmoke(EmbeddedElasticsearchNode embeddedElasticsearch)
            throws Exception
    {
        super(() -> ElasticsearchQueryRunner.createElasticsearchQueryRunner(embeddedElasticsearch, TpchTable.getTables()));
        this.embeddedElasticsearch = embeddedElasticsearch;
        this.embeddedElasticsearch.start();
    }

    @BeforeClass
    public void setUp()
    {
        queryRunner = getQueryRunner();
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
            throws IOException
    {
        try (Closer closer = Closer.create()) {
            closer.register(queryRunner);
            closer.register(embeddedElasticsearch);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Test
    @Override
    public void testDescribeTable()
    {
        MaterializedResult actualColumns = this.computeActual("DESC orders").toTestTypes();
        MaterializedResult.Builder builder = resultBuilder(this.getQueryRunner().getDefaultSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR);
        for (MaterializedRow row : actualColumns.getMaterializedRows()) {
            builder.row(row.getField(0), row.getField(1), "", "");
        }
        MaterializedResult filteredActual = builder.build();
        builder = resultBuilder(this.getQueryRunner().getDefaultSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR);
        MaterializedResult expectedColumns = builder
                .row("orderkey", "bigint", "", "")
                .row("custkey", "bigint", "", "")
                .row("orderstatus", "varchar", "", "")
                .row("totalprice", "double", "", "")
                .row("orderdate", "varchar", "", "")
                .row("orderpriority", "varchar", "", "")
                .row("clerk", "varchar", "", "")
                .row("shippriority", "bigint", "", "")
                .row("comment", "varchar", "", "").build();
        assertEquals(filteredActual, expectedColumns, String.format("%s != %s", filteredActual, expectedColumns));
    }
}
