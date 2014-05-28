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
package com.facebook.presto.operator.window;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static com.facebook.presto.operator.window.WindowAssertions.assertWindowQuery;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static java.util.Locale.ENGLISH;

public class TestWindowFunctions
{
    private static final ConnectorSession SESSION = new ConnectorSession("user", "test", "tpch", TINY_SCHEMA_NAME, UTC_KEY, ENGLISH, null, null);
    private final LocalQueryRunner queryRunner;

    public TestWindowFunctions()
    {
        queryRunner = new LocalQueryRunner(SESSION);
        queryRunner.createCatalog("tpch", new TpchConnectorFactory(queryRunner.getNodeManager(), 1), ImmutableMap.<String, String>of());
    }

    @AfterClass
    public void tearDown()
    {
        queryRunner.close();
    }

    @Test
    public void testRowNumber()
    {
        MaterializedResult expected = resultBuilder(SESSION, BIGINT, VARCHAR, BIGINT)
                .row(1, "O", 1)
                .row(2, "O", 2)
                .row(3, "F", 3)
                .row(4, "O", 4)
                .row(5, "F", 5)
                .row(6, "F", 6)
                .row(7, "O", 7)
                .row(32, "O", 8)
                .row(33, "F", 9)
                .row(34, "O", 10)
                .build();

        assertWindowQuery("row_number() OVER ()", expected, queryRunner);
        assertWindowQuery("row_number() OVER (ORDER BY orderkey)", expected, queryRunner);
    }

    @Test
    public void testRowNumberPartitioning()
    {
        assertWindowQuery("row_number() OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(SESSION, BIGINT, VARCHAR, BIGINT)
                        .row(3, "F", 1)
                        .row(5, "F", 2)
                        .row(6, "F", 3)
                        .row(33, "F", 4)
                        .row(1, "O", 1)
                        .row(2, "O", 2)
                        .row(4, "O", 3)
                        .row(7, "O", 4)
                        .row(32, "O", 5)
                        .row(34, "O", 6)
                        .build(), queryRunner
        );

        // TODO: add better test for non-deterministic sorting behavior
        assertWindowQuery("row_number() OVER (PARTITION BY orderstatus)",
                resultBuilder(SESSION, BIGINT, VARCHAR, BIGINT)
                        .row(3, "F", 1)
                        .row(5, "F", 2)
                        .row(33, "F", 3)
                        .row(6, "F", 4)
                        .row(32, "O", 1)
                        .row(34, "O", 2)
                        .row(1, "O", 3)
                        .row(2, "O", 4)
                        .row(4, "O", 5)
                        .row(7, "O", 6)
                        .build(), queryRunner
        );
    }

    @Test
    public void testRank()
    {
        assertWindowQuery("rank() OVER (ORDER BY orderstatus)",
                resultBuilder(SESSION, BIGINT, VARCHAR, BIGINT)
                        .row(3, "F", 1)
                        .row(5, "F", 1)
                        .row(6, "F", 1)
                        .row(33, "F", 1)
                        .row(1, "O", 5)
                        .row(2, "O", 5)
                        .row(4, "O", 5)
                        .row(7, "O", 5)
                        .row(32, "O", 5)
                        .row(34, "O", 5)
                        .build(), queryRunner
        );
    }

    @Test
    public void testDenseRank()
    {
        assertWindowQuery("dense_rank() OVER (ORDER BY orderstatus)",
                resultBuilder(SESSION, BIGINT, VARCHAR, BIGINT)
                        .row(3, "F", 1)
                        .row(5, "F", 1)
                        .row(6, "F", 1)
                        .row(33, "F", 1)
                        .row(1, "O", 2)
                        .row(2, "O", 2)
                        .row(4, "O", 2)
                        .row(7, "O", 2)
                        .row(32, "O", 2)
                        .row(34, "O", 2)
                        .build(), queryRunner
        );
    }

    @Test
    public void testPercentRank()
    {
        assertWindowQuery("percent_rank() OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(SESSION, BIGINT, VARCHAR, DOUBLE)
                        .row(3, "F", 0.0)
                        .row(5, "F", 1 / 3.0)
                        .row(6, "F", 2 / 3.0)
                        .row(33, "F", 1.0)
                        .row(1, "O", 0.0)
                        .row(2, "O", 0.2)
                        .row(4, "O", 0.4)
                        .row(7, "O", 0.6)
                        .row(32, "O", 0.8)
                        .row(34, "O", 1.0)
                        .build(), queryRunner
        );

        assertWindowQuery("percent_rank() OVER (ORDER BY orderkey)",
                resultBuilder(SESSION, BIGINT, VARCHAR, DOUBLE)
                        .row(1, "O", 0.0)
                        .row(2, "O", 1 / 9.0)
                        .row(3, "F", 2 / 9.0)
                        .row(4, "O", 3 / 9.0)
                        .row(5, "F", 4 / 9.0)
                        .row(6, "F", 5 / 9.0)
                        .row(7, "O", 6 / 9.0)
                        .row(32, "O", 7 / 9.0)
                        .row(33, "F", 8 / 9.0)
                        .row(34, "O", 1.0)
                        .build(), queryRunner
        );

        assertWindowQuery("percent_rank() OVER (ORDER BY orderstatus)",
                resultBuilder(SESSION, BIGINT, VARCHAR, DOUBLE)
                        .row(3, "F", 0.0)
                        .row(5, "F", 0.0)
                        .row(6, "F", 0.0)
                        .row(33, "F", 0.0)
                        .row(1, "O", 4 / 9.0)
                        .row(2, "O", 4 / 9.0)
                        .row(4, "O", 4 / 9.0)
                        .row(7, "O", 4 / 9.0)
                        .row(32, "O", 4 / 9.0)
                        .row(34, "O", 4 / 9.0)
                        .build(), queryRunner
        );

        assertWindowQuery("percent_rank() OVER (PARTITION BY orderkey)",
                resultBuilder(SESSION, BIGINT, VARCHAR, DOUBLE)
                        .row(1, "O", 0.0)
                        .row(2, "O", 0.0)
                        .row(3, "F", 0.0)
                        .row(4, "O", 0.0)
                        .row(5, "F", 0.0)
                        .row(6, "F", 0.0)
                        .row(7, "O", 0.0)
                        .row(32, "O", 0.0)
                        .row(33, "F", 0.0)
                        .row(34, "O", 0.0)
                        .build(), queryRunner
        );
    }

    @Test
    public void testCumulativeDistribution()
    {
        assertWindowQuery("cume_dist() OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(SESSION, BIGINT, VARCHAR, DOUBLE)
                        .row(3, "F", 0.25)
                        .row(5, "F", 0.5)
                        .row(6, "F", 0.75)
                        .row(33, "F", 1.0)
                        .row(1, "O", 1 / 6.0)
                        .row(2, "O", 2 / 6.0)
                        .row(4, "O", 3 / 6.0)
                        .row(7, "O", 4 / 6.0)
                        .row(32, "O", 5 / 6.0)
                        .row(34, "O", 1.0)
                        .build(), queryRunner
        );

        assertWindowQuery("cume_dist() OVER (ORDER BY orderkey)",
                resultBuilder(SESSION, BIGINT, VARCHAR, DOUBLE)
                        .row(1, "O", 0.1)
                        .row(2, "O", 0.2)
                        .row(3, "F", 0.3)
                        .row(4, "O", 0.4)
                        .row(5, "F", 0.5)
                        .row(6, "F", 0.6)
                        .row(7, "O", 0.7)
                        .row(32, "O", 0.8)
                        .row(33, "F", 0.9)
                        .row(34, "O", 1.0)
                        .build(), queryRunner
        );

        assertWindowQuery("cume_dist() OVER (ORDER BY orderstatus)",
                resultBuilder(SESSION, BIGINT, VARCHAR, DOUBLE)
                        .row(3, "F", 0.4)
                        .row(5, "F", 0.4)
                        .row(6, "F", 0.4)
                        .row(33, "F", 0.4)
                        .row(1, "O", 1.0)
                        .row(2, "O", 1.0)
                        .row(4, "O", 1.0)
                        .row(7, "O", 1.0)
                        .row(32, "O", 1.0)
                        .row(34, "O", 1.0)
                        .build(), queryRunner
        );

        assertWindowQuery("cume_dist() OVER (PARTITION BY orderkey)",
                resultBuilder(SESSION, BIGINT, VARCHAR, DOUBLE)
                        .row(1, "O", 1.0)
                        .row(2, "O", 1.0)
                        .row(3, "F", 1.0)
                        .row(4, "O", 1.0)
                        .row(5, "F", 1.0)
                        .row(6, "F", 1.0)
                        .row(7, "O", 1.0)
                        .row(32, "O", 1.0)
                        .row(33, "F", 1.0)
                        .row(34, "O", 1.0)
                        .build(), queryRunner
        );
    }
}
