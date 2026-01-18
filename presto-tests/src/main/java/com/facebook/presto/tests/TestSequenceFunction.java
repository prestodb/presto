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
package com.facebook.presto.tests;

import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tpch.TpchPlugin;
import org.testng.annotations.Test;

import static com.facebook.presto.operator.table.Sequence.SequenceFunctionSplit.DEFAULT_SPLIT_SIZE;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static java.lang.String.format;

public class TestSequenceFunction
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(testSessionBuilder().build()).build();
        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");
        return queryRunner;
    }

    @Override
    protected QueryRunner createExpectedQueryRunner()
            throws Exception
    {
        DistributedQueryRunner result = DistributedQueryRunner.builder(testSessionBuilder()
                        .setCatalog("tpch")
                        .setSchema(TINY_SCHEMA_NAME)
                        .build())
                .build();
        result.installPlugin(new TpchPlugin());
        result.createCatalog("tpch", "tpch");
        return result;
    }

    @Test
    public void testSequence()
    {
        assertQuery("SELECT * FROM TABLE(sequence(0, 8000, 3))",
                "SELECT * FROM UNNEST(sequence(0, 8000, 3))");

        assertQuery("SELECT * FROM TABLE(sequence(1, 10, 3))",
                "VALUES BIGINT '1', 4, 7, 10");

        assertQuery("SELECT * FROM TABLE(sequence(1, 10, 6))",
                "VALUES BIGINT '1', 7");

        assertQuery("SELECT * FROM TABLE(sequence(-1, -10, -3))",
                "VALUES BIGINT '-1', -4, -7, -10");

        assertQuery("SELECT * FROM TABLE(sequence(-1, -10, -6))",
                "VALUES BIGINT '-1', -7");

        assertQuery("SELECT * FROM TABLE(sequence(-5, 5, 3))",
                "VALUES BIGINT '-5', -2, 1, 4");

        assertQuery("SELECT * FROM TABLE(sequence(5, -5, -3))",
                "VALUES BIGINT '5', 2, -1, -4");

        assertQuery("SELECT * FROM TABLE(sequence(0, 10, 3))",
                "VALUES BIGINT '0', 3, 6, 9");

        assertQuery("SELECT * FROM TABLE(sequence(0, -10, -3))",
                "VALUES BIGINT '0', -3, -6, -9");
    }

    @Test
    public void testDefaultArguments()
    {
        assertQuery("SELECT * FROM TABLE(sequence(stop => 10))",
                "SELECT * FROM UNNEST(sequence(0, 10, 1))");
    }

    @Test
    public void testInvalidArgument()
    {
        assertQueryFailsExact("SELECT * " +
                        "FROM TABLE(sequence( " +
                        "                    start => -5," +
                        "                    stop => 10," +
                        "                    step => -2))",
                "Step must be positive for sequence [-5, 10]");

        assertQueryFailsExact("SELECT * " +
                        "FROM TABLE(sequence(" +
                        "                    start => 10," +
                        "                    stop => -5," +
                        "                    step => 2))",
                "Step must be negative for sequence [10, -5]");

        assertQueryFailsExact("SELECT * " +
                        "FROM TABLE(sequence(" +
                        "                    start => null," +
                        "                    stop => -5," +
                        "                    step => 2))",
                "Start is null");

        assertQueryFailsExact("SELECT * " +
                        "FROM TABLE(sequence(" +
                        "                    start => 10," +
                        "                    stop => null," +
                        "                    step => 2))",
                "Stop is null");

        assertQueryFailsExact("SELECT * " +
                        "FROM TABLE(sequence(" +
                        "                    start => 10," +
                        "                    stop => -5," +
                        "                    step => null))",
                "Step is null");
    }

    @Test
    public void testSingletonSequence()
    {
        assertQuery("SELECT * " +
                        "FROM TABLE(sequence(" +
                        "                    start => 10," +
                        "                    stop => 10," +
                        "                    step => 2))",
                "VALUES BIGINT '10'");

        assertQuery("SELECT * " +
                        "FROM TABLE(sequence(" +
                        "                    start => 10," +
                        "                    stop => 10," +
                        "                    step => -2))",
                "VALUES BIGINT '10'");

        assertQuery("SELECT * " +
                        "FROM TABLE(sequence(" +
                        "                    start => 10," +
                        "                    stop => 10," +
                        "                    step => 0))",
                "VALUES BIGINT '10'");
    }

    @Test
    public void testBigStep()
    {
        assertQuery(format("SELECT * " +
                        "FROM TABLE(sequence(" +
                        "                    start => 10," +
                        "                    stop => -5," +
                        "                    step => %s))",
                Long.MIN_VALUE / (DEFAULT_SPLIT_SIZE - 1)), "VALUES BIGINT '10'");

        assertQuery(format("SELECT * " +
                                "FROM TABLE(sequence(" +
                                "                    start => 10," +
                                "                    stop => -5," +
                                "                    step => %s))",
                        Long.MIN_VALUE / (DEFAULT_SPLIT_SIZE - 1) - 1),
                "VALUES BIGINT '10'");

        assertQuery(format("SELECT DISTINCT x - lag(x, 1) OVER(ORDER BY x DESC) \n" +
                                "FROM TABLE(sequence(\n" +
                                "                    start => %s,\n" +
                                "                    stop => BIGINT '%s',\n" +
                                "                    step => %s)) t(x)",
                        Long.MAX_VALUE, Long.MIN_VALUE, Long.MIN_VALUE / (DEFAULT_SPLIT_SIZE - 1) - 1),
                format("VALUES (null), (%s)", Long.MIN_VALUE / (DEFAULT_SPLIT_SIZE - 1) - 1));

        assertQuery(format("SELECT * " +
                        "FROM TABLE(sequence(" +
                        "                    start => 10," +
                        "                    stop => -5," +
                        "                    step => BIGINT '%s'))", Long.MIN_VALUE),
                "VALUES BIGINT '10'");

        assertQuery(format("SELECT * " +
                        "FROM TABLE(sequence(" +
                        "                    start => -5," +
                        "                    stop => 10," +
                        "                    step => %s))", Long.MAX_VALUE / (DEFAULT_SPLIT_SIZE - 1)),
                "VALUES BIGINT '-5'");

        assertQuery(format("SELECT * " +
                        "FROM TABLE(sequence(" +
                        "                    start => -5," +
                        "                    stop => 10," +
                        "                    step => %s))", Long.MAX_VALUE / (DEFAULT_SPLIT_SIZE - 1) + 1),
                "VALUES BIGINT '-5'");

        assertQuery(format("SELECT DISTINCT x - lag(x, 1) OVER(ORDER BY x) " +
                                "FROM TABLE(sequence(" +
                                "                    start => BIGINT '%s'," +
                                "                    stop => %s," +
                                "                    step => %s)) t(x)",
                        Long.MIN_VALUE, Long.MAX_VALUE, Long.MAX_VALUE / (DEFAULT_SPLIT_SIZE - 1) + 1),
                format("VALUES (null), (%s)", Long.MAX_VALUE / (DEFAULT_SPLIT_SIZE - 1) + 1));

        assertQuery(format("SELECT * " +
                        "FROM TABLE(sequence(" +
                        "                    start => -5," +
                        "                    stop => 10," +
                        "                    step => %s))", Long.MAX_VALUE),
                "VALUES BIGINT '-5'");
    }

    @Test
    public void testMultipleSplits()
    {
        long sequenceLength = DEFAULT_SPLIT_SIZE * 10 + DEFAULT_SPLIT_SIZE / 2;
        long start = 10;
        long step = 5;
        long stop = start + (sequenceLength - 1) * step;
        assertQuery(format("SELECT count(x), count(DISTINCT x), min(x), max(x) " +
                        "FROM TABLE(sequence( " +
                        "                    start => %s," +
                        "                    stop => %s," +
                        "                    step => %s)) t(x)", start, stop, step),
                format("SELECT BIGINT '%s', BIGINT '%s', BIGINT '%s', BIGINT '%s'", sequenceLength, sequenceLength, start, stop));

        sequenceLength = DEFAULT_SPLIT_SIZE * 4 + DEFAULT_SPLIT_SIZE / 2;
        stop = start + (sequenceLength - 1) * step;
        assertQuery(format("SELECT min(x), max(x) " +
                        "FROM TABLE(sequence(" +
                        "                    start => %s," +
                        "                    stop => %s," +
                        "                    step => %s)) t(x)", start, stop, step),
                format("SELECT BIGINT '%s', BIGINT '%s'", start, stop));

        step = -5;
        stop = start + (sequenceLength - 1) * step;
        assertQuery(format("SELECT max(x), min(x) " +
                        "FROM TABLE(sequence(" +
                        "                    start => %s," +
                        "                    stop => %s," +
                        "                    step => %s)) t(x)", start, stop, step),
                format("SELECT BIGINT '%s', BIGINT '%s'", start, stop));
    }

    @Test
    public void testEdgeValues()
    {
        long start = Long.MIN_VALUE + 15;
        long stop = Long.MIN_VALUE + 3;
        long step = -10;
        assertQuery(format("SELECT * " +
                        "FROM TABLE(sequence( " +
                        "                    start => %s," +
                        "                    stop => %s," +
                        "                    step => %s))", start, stop, step),
                format("VALUES (%s), (%s)", start, start + step));

        start = Long.MIN_VALUE + 1 - (DEFAULT_SPLIT_SIZE - 1) * step;
        stop = Long.MIN_VALUE + 1;
        assertQuery(format("SELECT max(x), min(x) " +
                        "FROM TABLE(sequence( " +
                        "                    start => %s," +
                        "                    stop => %s," +
                        "                    step => %s)) t(x)", start, stop, step),
                format("SELECT %s, %s", start, Long.MIN_VALUE + 1));

        start = Long.MAX_VALUE - 15;
        stop = Long.MAX_VALUE - 3;
        step = 10;
        assertQuery(format("SELECT * " +
                        "FROM TABLE(sequence( " +
                        "                    start => %s," +
                        "                    stop => %s," +
                        "                    step => %s))", start, stop, step),
                format("VALUES (%s), (%s)", start, start + step));

        start = Long.MAX_VALUE - 1 - (DEFAULT_SPLIT_SIZE - 1) * step;
        stop = Long.MAX_VALUE - 1;
        assertQuery(format("SELECT min(x), max(x) " +
                        "FROM TABLE(sequence(" +
                        "                    start => %s," +
                        "                    stop => %s," +
                        "                    step => %s)) t(x)", start, stop, step),
                format("SELECT %s, %s", start, Long.MAX_VALUE - 1));
    }
}
