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

import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.MaterializedResult;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.operator.window.WindowAssertions.assertWindowQuery;
import static com.facebook.presto.operator.window.WindowAssertions.assertWindowQueryWithNulls;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;

public class TestWindowFunctions
{
    private final LocalQueryRunner queryRunner;

    public TestWindowFunctions()
    {
        queryRunner = new LocalQueryRunner(TEST_SESSION);
    }

    @AfterClass
    public void tearDown()
    {
        queryRunner.close();
    }

    @Test
    public void testRowNumber()
    {
        MaterializedResult expected = resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
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
        MaterializedResult expectedWithNulls = resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
                .row(1, null, 1)
                .row(3, "F", 2)
                .row(5, "F", 3)
                .row(7, null, 4)
                .row(34, "O", 5)
                .row(null, "F", 6)
                .row(null, "F", 7)
                .row(null, "O", 8)
                .row(null, null, 9)
                .row(null, null, 10)
                .build();

        assertWindowQuery("row_number() OVER ()", expected, queryRunner);
        assertWindowQuery("row_number() OVER (ORDER BY orderkey)", expected, queryRunner);

        assertWindowQueryWithNulls("row_number() OVER ()", expectedWithNulls, queryRunner);
        assertWindowQueryWithNulls("row_number() OVER (ORDER BY orderkey)", expectedWithNulls, queryRunner);
    }

    @Test
    public void testRank()
    {
        assertWindowQuery("rank() OVER (ORDER BY orderstatus)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
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
        assertWindowQueryWithNulls("rank() OVER (ORDER BY orderstatus)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
                        .row(3, "F", 1)
                        .row(5, "F", 1)
                        .row(null, "F", 1)
                        .row(null, "F", 1)
                        .row(34, "O", 5)
                        .row(null, "O", 5)
                        .row(1, null, 7)
                        .row(7, null, 7)
                        .row(null, null, 7)
                        .row(null, null, 7)
                        .build(), queryRunner
        );
    }

    @Test
    public void testDenseRank()
    {
        assertWindowQuery("dense_rank() OVER (ORDER BY orderstatus)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
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
        assertWindowQueryWithNulls("dense_rank() OVER (ORDER BY orderstatus)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
                        .row(3, "F", 1)
                        .row(5, "F", 1)
                        .row(null, "F", 1)
                        .row(null, "F", 1)
                        .row(34, "O", 2)
                        .row(null, "O", 2)
                        .row(1, null, 3)
                        .row(7, null, 3)
                        .row(null, null, 3)
                        .row(null, null, 3)
                        .build(), queryRunner
        );
    }

    @Test
    public void testPercentRank()
    {
        assertWindowQuery("percent_rank() OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, DOUBLE)
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
        assertWindowQueryWithNulls("percent_rank() OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
                        .row(3, "F", 0.0)
                        .row(5, "F", 1 / 3.0)
                        .row(null, "F", 2 / 3.0)
                        .row(null, "F", 2 / 3.0)
                        .row(34, "O", 0.0)
                        .row(null, "O", 1.0)
                        .row(1, null, 0.0)
                        .row(7, null, 1 / 3.0)
                        .row(null, null, 2 / 3.0)
                        .row(null, null, 2 / 3.0)
                        .build(), queryRunner
        );

        assertWindowQuery("percent_rank() OVER (ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, DOUBLE)
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
        assertWindowQueryWithNulls("percent_rank() OVER (ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
                        .row(1, null, 0.0)
                        .row(3, "F", 1 / 9.0)
                        .row(5, "F", 2 / 9.0)
                        .row(7, null, 3 / 9.0)
                        .row(34, "O", 4 / 9.0)
                        .row(null, "F", 5 / 9.0)
                        .row(null, "F", 5 / 9.0)
                        .row(null, "O", 5 / 9.0)
                        .row(null, null, 5 / 9.0)
                        .row(null, null, 5 / 9.0)
                        .build(), queryRunner
        );

        assertWindowQuery("percent_rank() OVER (ORDER BY orderstatus)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, DOUBLE)
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
        assertWindowQueryWithNulls("percent_rank() OVER (ORDER BY orderstatus)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
                        .row(3, "F", 0.0)
                        .row(5, "F", 0.0)
                        .row(null, "F", 0.0)
                        .row(null, "F", 0.0)
                        .row(34, "O", 4 / 9.0)
                        .row(null, "O", 4 / 9.0)
                        .row(1, null, 6 / 9.0)
                        .row(7, null, 6 / 9.0)
                        .row(null, null, 6 / 9.0)
                        .row(null, null, 6 / 9.0)
                        .build(), queryRunner
        );

        assertWindowQuery("percent_rank() OVER (PARTITION BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, DOUBLE)
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
        assertWindowQueryWithNulls("percent_rank() OVER (PARTITION BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
                        .row(1, null, 0.0)
                        .row(3, "F", 0.0)
                        .row(5, "F", 0.0)
                        .row(7, null, 0.0)
                        .row(34, "O", 0.0)
                        .row(null, "F", 0.0)
                        .row(null, "F", 0.0)
                        .row(null, "O", 0.0)
                        .row(null, null, 0.0)
                        .row(null, null, 0.0)
                        .build(), queryRunner
        );
    }

    @Test
    public void testCumulativeDistribution()
    {
        assertWindowQuery("cume_dist() OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, DOUBLE)
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
        assertWindowQueryWithNulls("cume_dist() OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, DOUBLE)
                        .row(3, "F", 0.25)
                        .row(5, "F", 0.5)
                        .row(null, "F", 1.0)
                        .row(null, "F", 1.0)
                        .row(34, "O", 0.5)
                        .row(null, "O", 1.0)
                        .row(1, null, 0.25)
                        .row(7, null, 0.5)
                        .row(null, null, 1.0)
                        .row(null, null, 1.0)
                        .build(), queryRunner
        );

        assertWindowQuery("cume_dist() OVER (ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, DOUBLE)
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
        assertWindowQueryWithNulls("cume_dist() OVER (ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, DOUBLE)
                        .row(1, null, 0.1)
                        .row(3, "F", 0.2)
                        .row(5, "F", 0.3)
                        .row(7, null, 0.4)
                        .row(34, "O", 0.5)
                        .row(null, "F", 1.0)
                        .row(null, "F", 1.0)
                        .row(null, "O", 1.0)
                        .row(null, null, 1.0)
                        .row(null, null, 1.0)
                        .build(), queryRunner
        );

        assertWindowQuery("cume_dist() OVER (ORDER BY orderstatus)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, DOUBLE)
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
        assertWindowQueryWithNulls("cume_dist() OVER (ORDER BY orderstatus)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, DOUBLE)
                        .row(3, "F", 0.4)
                        .row(5, "F", 0.4)
                        .row(null, "F", 0.4)
                        .row(null, "F", 0.4)
                        .row(34, "O", 0.6)
                        .row(null, "O", 0.6)
                        .row(1, null, 1.0)
                        .row(7, null, 1.0)
                        .row(null, null, 1.0)
                        .row(null, null, 1.0)
                        .build(), queryRunner
        );

        assertWindowQuery("cume_dist() OVER (PARTITION BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, DOUBLE)
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
        assertWindowQueryWithNulls("cume_dist() OVER (PARTITION BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, DOUBLE)
                        .row(1, null, 1.0)
                        .row(3, "F", 1.0)
                        .row(5, "F", 1.0)
                        .row(7, null, 1.0)
                        .row(34, "O", 1.0)
                        .row(null, "F", 1.0)
                        .row(null, "F", 1.0)
                        .row(null, "O", 1.0)
                        .row(null, null, 1.0)
                        .row(null, null, 1.0)
                        .build(), queryRunner
        );
    }

    @Test
    public void testFirstValue()
    {
        assertWindowQuery("first_value(orderdate) OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, VARCHAR)
                        .row(3, "F", "1993-10-14")
                        .row(5, "F", "1993-10-14")
                        .row(6, "F", "1993-10-14")
                        .row(33, "F", "1993-10-14")
                        .row(1, "O", "1996-01-02")
                        .row(2, "O", "1996-01-02")
                        .row(4, "O", "1996-01-02")
                        .row(7, "O", "1996-01-02")
                        .row(32, "O", "1996-01-02")
                        .row(34, "O", "1996-01-02")
                        .build(), queryRunner);
        assertWindowQueryWithNulls("first_value(orderdate) OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, VARCHAR)
                        .row(3, "F", "1993-10-14")
                        .row(5, "F", "1993-10-14")
                        .row(null, "F", "1993-10-14")
                        .row(null, "F", "1993-10-14")
                        .row(34, "O", "1998-07-21")
                        .row(null, "O", "1998-07-21")
                        .row(1, null, null)
                        .row(7, null, null)
                        .row(null, null, null)
                        .row(null, null, null)
                        .build(), queryRunner);

        assertWindowQuery("first_value(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
                        .row(3, "F", 3)
                        .row(5, "F", 3)
                        .row(6, "F", 3)
                        .row(33, "F", 3)
                        .row(1, "O", 1)
                        .row(2, "O", 1)
                        .row(4, "O", 1)
                        .row(7, "O", 1)
                        .row(32, "O", 1)
                        .row(34, "O", 1)
                        .build(), queryRunner);
        assertWindowQueryWithNulls("first_value(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
                        .row(3, "F", 3)
                        .row(5, "F", 3)
                        .row(null, "F", 3)
                        .row(null, "F", 3)
                        .row(34, "O", 34)
                        .row(null, "O", 34)
                        .row(1, null, 1)
                        .row(7, null, 1)
                        .row(null, null, 1)
                        .row(null, null, 1)
                        .build(), queryRunner);
    }

    @Test
    public void testLastValue()
    {
        assertWindowQuery("last_value(orderdate) OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, VARCHAR)
                        .row(3, "F", "1993-10-27")
                        .row(5, "F", "1993-10-27")
                        .row(6, "F", "1993-10-27")
                        .row(33, "F", "1993-10-27")
                        .row(1, "O", "1998-07-21")
                        .row(2, "O", "1998-07-21")
                        .row(4, "O", "1998-07-21")
                        .row(7, "O", "1998-07-21")
                        .row(32, "O", "1998-07-21")
                        .row(34, "O", "1998-07-21")
                        .build(), queryRunner);
        assertWindowQueryWithNulls("last_value(orderdate) OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, VARCHAR)
                        .row(3, "F", "1992-02-21")
                        .row(5, "F", "1992-02-21")
                        .row(null, "F", "1992-02-21")
                        .row(null, "F", "1992-02-21")
                        .row(34, "O", "1996-12-01")
                        .row(null, "O", "1996-12-01")
                        .row(1, null, "1995-07-16")
                        .row(7, null, "1995-07-16")
                        .row(null, null, "1995-07-16")
                        .row(null, null, "1995-07-16")
                        .build(), queryRunner);

        assertWindowQuery("last_value(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
                        .row(3, "F", 33)
                        .row(5, "F", 33)
                        .row(6, "F", 33)
                        .row(33, "F", 33)
                        .row(1, "O", 34)
                        .row(2, "O", 34)
                        .row(4, "O", 34)
                        .row(7, "O", 34)
                        .row(32, "O", 34)
                        .row(34, "O", 34)
                        .build(), queryRunner);
        assertWindowQueryWithNulls("last_value(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
                        .row(3, "F", null)
                        .row(5, "F", null)
                        .row(null, "F", null)
                        .row(null, "F", null)
                        .row(34, "O", null)
                        .row(null, "O", null)
                        .row(1, null, null)
                        .row(7, null, null)
                        .row(null, null, null)
                        .row(null, null, null)
                        .build(), queryRunner);
    }

    @Test
    public void testLagFunction()
    {
        assertWindowQuery("lag(orderdate) OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, VARCHAR)
                        .row(3, "F", null)
                        .row(5, "F", "1993-10-14")
                        .row(6, "F", "1994-07-30")
                        .row(33, "F", "1992-02-21")
                        .row(1, "O", null)
                        .row(2, "O", "1996-01-02")
                        .row(4, "O", "1996-12-01")
                        .row(7, "O", "1995-10-11")
                        .row(32, "O", "1996-01-10")
                        .row(34, "O", "1995-07-16")
                        .build(), queryRunner);
        assertWindowQueryWithNulls("lag(orderdate) OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, VARCHAR)
                        .row(3, "F", null)
                        .row(5, "F", "1993-10-14")
                        .row(null, "F", null)
                        .row(null, "F", "1993-10-27")
                        .row(34, "O", null)
                        .row(null, "O", "1998-07-21")
                        .row(1, null, null)
                        .row(7, null, null)
                        .row(null, null, "1996-01-10")
                        .row(null, null, null)
                        .build(), queryRunner);

        assertWindowQuery("lag(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
                        .row(3, "F", null)
                        .row(5, "F", 3)
                        .row(6, "F", 5)
                        .row(33, "F", 6)
                        .row(1, "O", null)
                        .row(2, "O", 1)
                        .row(4, "O", 2)
                        .row(7, "O", 4)
                        .row(32, "O", 7)
                        .row(34, "O", 32)
                        .build(), queryRunner);
        assertWindowQueryWithNulls("lag(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
                        .row(3, "F", null)
                        .row(5, "F", 3)
                        .row(null, "F", 5)
                        .row(null, "F", null)
                        .row(34, "O", null)
                        .row(null, "O", 34)
                        .row(1, null, null)
                        .row(7, null, 1)
                        .row(null, null, 7)
                        .row(null, null, null)
                        .build(), queryRunner);

        assertWindowQuery("lag(orderdate, 2, '1977-01-01') OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, VARCHAR)
                        .row(3, "F", "1977-01-01")
                        .row(5, "F", "1977-01-01")
                        .row(6, "F", "1993-10-14")
                        .row(33, "F", "1994-07-30")
                        .row(1, "O", "1977-01-01")
                        .row(2, "O", "1977-01-01")
                        .row(4, "O", "1996-01-02")
                        .row(7, "O", "1996-12-01")
                        .row(32, "O", "1995-10-11")
                        .row(34, "O", "1996-01-10")
                        .build(), queryRunner);
        assertWindowQueryWithNulls("lag(orderdate, 2, '1977-01-01') OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, VARCHAR)
                        .row(3, "F", "1977-01-01")
                        .row(5, "F", "1977-01-01")
                        .row(null, "F", "1993-10-14")
                        .row(null, "F", null)
                        .row(34, "O", "1977-01-01")
                        .row(null, "O", "1977-01-01")
                        .row(1, null, "1977-01-01")
                        .row(7, null, "1977-01-01")
                        .row(null, null, null)
                        .row(null, null, "1996-01-10")
                        .build(), queryRunner);

        assertWindowQuery("lag(orderkey, 2, -1) OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
                        .row(3, "F", -1)
                        .row(5, "F", -1)
                        .row(6, "F", 3)
                        .row(33, "F", 5)
                        .row(1, "O", -1)
                        .row(2, "O", -1)
                        .row(4, "O", 1)
                        .row(7, "O", 2)
                        .row(32, "O", 4)
                        .row(34, "O", 7)
                        .build(), queryRunner);
        assertWindowQueryWithNulls("lag(orderkey, 2, -1) OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
                        .row(3, "F", -1)
                        .row(5, "F", -1)
                        .row(null, "F", 3)
                        .row(null, "F", 5)
                        .row(34, "O", -1)
                        .row(null, "O", -1)
                        .row(1, null, -1)
                        .row(7, null, -1)
                        .row(null, null, 1)
                        .row(null, null, 7)
                        .build(), queryRunner);
    }

    @Test
    public void testLeadFunction()
    {
        assertWindowQuery("lead(orderdate) OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, VARCHAR)
                        .row(3, "F", "1994-07-30")
                        .row(5, "F", "1992-02-21")
                        .row(6, "F", "1993-10-27")
                        .row(33, "F", null)
                        .row(1, "O", "1996-12-01")
                        .row(2, "O", "1995-10-11")
                        .row(4, "O", "1996-01-10")
                        .row(7, "O", "1995-07-16")
                        .row(32, "O", "1998-07-21")
                        .row(34, "O", null)
                        .build(), queryRunner);
        assertWindowQueryWithNulls("lead(orderdate) OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, VARCHAR)
                        .row(3, "F", null)
                        .row(5, "F", "1993-10-27")
                        .row(null, "F", "1992-02-21")
                        .row(null, "F", null)
                        .row(34, "O", "1996-12-01")
                        .row(null, "O", null)
                        .row(1, null, "1996-01-10")
                        .row(7, null, null)
                        .row(null, null, "1995-07-16")
                        .row(null, null, null)
                        .build(), queryRunner);

        assertWindowQuery("lead(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
                        .row(3, "F", 5)
                        .row(5, "F", 6)
                        .row(6, "F", 33)
                        .row(33, "F", null)
                        .row(1, "O", 2)
                        .row(2, "O", 4)
                        .row(4, "O", 7)
                        .row(7, "O", 32)
                        .row(32, "O", 34)
                        .row(34, "O", null)
                        .build(), queryRunner);
        assertWindowQueryWithNulls("lead(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
                        .row(3, "F", 5)
                        .row(5, "F", null)
                        .row(null, "F", null)
                        .row(null, "F", null)
                        .row(34, "O", null)
                        .row(null, "O", null)
                        .row(1, null, 7)
                        .row(7, null, null)
                        .row(null, null, null)
                        .row(null, null, null)
                        .build(), queryRunner);

        assertWindowQuery("lead(orderdate, 2, '1977-01-01') OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, VARCHAR)
                        .row(3, "F", "1992-02-21")
                        .row(5, "F", "1993-10-27")
                        .row(6, "F", "1977-01-01")
                        .row(33, "F", "1977-01-01")
                        .row(1, "O", "1995-10-11")
                        .row(2, "O", "1996-01-10")
                        .row(4, "O", "1995-07-16")
                        .row(7, "O", "1998-07-21")
                        .row(32, "O", "1977-01-01")
                        .row(34, "O", "1977-01-01")
                        .build(), queryRunner);
        assertWindowQueryWithNulls("lead(orderdate, 2, '1977-01-01') OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, VARCHAR)
                        .row(3, "F", "1993-10-27")
                        .row(5, "F", "1992-02-21")
                        .row(null, "F", "1977-01-01")
                        .row(null, "F", "1977-01-01")
                        .row(34, "O", "1977-01-01")
                        .row(null, "O", "1977-01-01")
                        .row(1, null, null)
                        .row(7, null, "1995-07-16")
                        .row(null, null, "1977-01-01")
                        .row(null, null, "1977-01-01")
                        .build(), queryRunner);

        assertWindowQuery("lead(orderkey, 2, -1) OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
                        .row(3, "F", 6)
                        .row(5, "F", 33)
                        .row(6, "F", -1)
                        .row(33, "F", -1)
                        .row(1, "O", 4)
                        .row(2, "O", 7)
                        .row(4, "O", 32)
                        .row(7, "O", 34)
                        .row(32, "O", -1)
                        .row(34, "O", -1)
                        .build(), queryRunner);
        assertWindowQueryWithNulls("lead(orderkey, 2, -1) OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
                        .row(3, "F", null)
                        .row(5, "F", null)
                        .row(null, "F", -1)
                        .row(null, "F", -1)
                        .row(34, "O", -1)
                        .row(null, "O", -1)
                        .row(1, null, null)
                        .row(7, null, null)
                        .row(null, null, -1)
                        .row(null, null, -1)
                        .build(), queryRunner);
    }
}
