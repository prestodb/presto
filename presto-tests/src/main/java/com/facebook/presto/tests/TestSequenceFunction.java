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

    @Test
    public void testUnionAllWithMultipleSequences()
    {
        // Test UNION ALL with two sequences
        assertQuery("SELECT 'Even Numbers' AS sequence_type, num AS value " +
                        "FROM TABLE(sequence(0, 100, 2)) AS t(num) " +
                        "UNION ALL " +
                        "SELECT 'Multiples of 5' AS sequence_type, num AS value " +
                        "FROM TABLE(sequence(0, 100, 5)) AS t(num) " +
                        "ORDER BY sequence_type, value",
                "SELECT * FROM (VALUES " +
                        "('Even Numbers', BIGINT '0'), ('Even Numbers', 2), ('Even Numbers', 4), ('Even Numbers', 6), ('Even Numbers', 8), ('Even Numbers', 10), " +
                        "('Even Numbers', 12), ('Even Numbers', 14), ('Even Numbers', 16), ('Even Numbers', 18), ('Even Numbers', 20), " +
                        "('Even Numbers', 22), ('Even Numbers', 24), ('Even Numbers', 26), ('Even Numbers', 28), ('Even Numbers', 30), " +
                        "('Even Numbers', 32), ('Even Numbers', 34), ('Even Numbers', 36), ('Even Numbers', 38), ('Even Numbers', 40), " +
                        "('Even Numbers', 42), ('Even Numbers', 44), ('Even Numbers', 46), ('Even Numbers', 48), ('Even Numbers', 50), " +
                        "('Even Numbers', 52), ('Even Numbers', 54), ('Even Numbers', 56), ('Even Numbers', 58), ('Even Numbers', 60), " +
                        "('Even Numbers', 62), ('Even Numbers', 64), ('Even Numbers', 66), ('Even Numbers', 68), ('Even Numbers', 70), " +
                        "('Even Numbers', 72), ('Even Numbers', 74), ('Even Numbers', 76), ('Even Numbers', 78), ('Even Numbers', 80), " +
                        "('Even Numbers', 82), ('Even Numbers', 84), ('Even Numbers', 86), ('Even Numbers', 88), ('Even Numbers', 90), " +
                        "('Even Numbers', 92), ('Even Numbers', 94), ('Even Numbers', 96), ('Even Numbers', 98), ('Even Numbers', 100), " +
                        "('Multiples of 5', BIGINT '0'), ('Multiples of 5', 5), ('Multiples of 5', 10), ('Multiples of 5', 15), ('Multiples of 5', 20), " +
                        "('Multiples of 5', 25), ('Multiples of 5', 30), ('Multiples of 5', 35), ('Multiples of 5', 40), ('Multiples of 5', 45), " +
                        "('Multiples of 5', 50), ('Multiples of 5', 55), ('Multiples of 5', 60), ('Multiples of 5', 65), ('Multiples of 5', 70), " +
                        "('Multiples of 5', 75), ('Multiples of 5', 80), ('Multiples of 5', 85), ('Multiples of 5', 90), ('Multiples of 5', 95), " +
                        "('Multiples of 5', 100)) AS t(sequence_type, value) " +
                        "ORDER BY sequence_type, value");

        // Test UNION ALL with three sequences (original failing query)
        assertQuery("SELECT 'Even Numbers' AS sequence_type, num AS value " +
                        "FROM TABLE(sequence(0, 100, 2)) AS t(num) " +
                        "UNION ALL " +
                        "SELECT 'Multiples of 5' AS sequence_type, num AS value " +
                        "FROM TABLE(sequence(0, 100, 5)) AS t(num) " +
                        "UNION ALL " +
                        "SELECT 'Multiples of 10' AS sequence_type, num AS value " +
                        "FROM TABLE(sequence(0, 100, 10)) AS t(num) " +
                        "ORDER BY sequence_type, value",
                "SELECT sequence_type, value FROM (" +
                        "SELECT 'Even Numbers' AS sequence_type, x AS value FROM UNNEST(sequence(0, 100, 2)) AS t(x) " +
                        "UNION ALL " +
                        "SELECT 'Multiples of 5', x FROM UNNEST(sequence(0, 100, 5)) AS t(x) " +
                        "UNION ALL " +
                        "SELECT 'Multiples of 10', x FROM UNNEST(sequence(0, 100, 10)) AS t(x)) " +
                        "ORDER BY sequence_type, value");
    }

    @Test
    public void testUnionAllWithAggregation()
    {
        // Test UNION ALL with aggregation to verify correct result counts
        assertQuery("SELECT sequence_type, COUNT(*) AS cnt, MIN(value) AS min_val, MAX(value) AS max_val " +
                        "FROM (" +
                        "  SELECT 'Even Numbers' AS sequence_type, num AS value " +
                        "  FROM TABLE(sequence(0, 100, 2)) AS t(num) " +
                        "  UNION ALL " +
                        "  SELECT 'Odd Numbers' AS sequence_type, num AS value " +
                        "  FROM TABLE(sequence(1, 99, 2)) AS t(num)" +
                        ") " +
                        "GROUP BY sequence_type " +
                        "ORDER BY sequence_type",
                "VALUES ('Even Numbers', BIGINT '51', BIGINT '0', BIGINT '100'), " +
                        "('Odd Numbers', BIGINT '50', BIGINT '1', BIGINT '99')");
    }

    @Test
    public void testUnionAllWithLargeSequences()
    {
        // Test UNION ALL with sequences that generate multiple splits
        long sequenceLength = DEFAULT_SPLIT_SIZE * 2;
        long start1 = 0;
        long step1 = 1;
        long stop1 = start1 + (sequenceLength - 1) * step1;

        long start2 = 1000000;
        long step2 = 1;
        long stop2 = start2 + (sequenceLength - 1) * step2;

        assertQuery(format("SELECT sequence_type, COUNT(*) AS cnt, MIN(value) AS min_val, MAX(value) AS max_val " +
                                "FROM (" +
                                "  SELECT 'First' AS sequence_type, num AS value " +
                                "  FROM TABLE(sequence(%s, %s, %s)) AS t(num) " +
                                "  UNION ALL " +
                                "  SELECT 'Second' AS sequence_type, num AS value " +
                                "  FROM TABLE(sequence(%s, %s, %s)) AS t(num)" +
                                ") " +
                                "GROUP BY sequence_type " +
                                "ORDER BY sequence_type",
                        start1, stop1, step1, start2, stop2, step2),
                format("VALUES ('First', BIGINT '%s', BIGINT '%s', BIGINT '%s'), " +
                                "('Second', BIGINT '%s', BIGINT '%s', BIGINT '%s')",
                        sequenceLength, start1, stop1, sequenceLength, start2, stop2));
    }

    @Test
    public void testUnionAllWithJoin()
    {
        // Test UNION ALL sequences joined with a table
        assertQuery("SELECT t.name, s.value " +
                        "FROM tpch.tiny.nation t " +
                        "JOIN (" +
                        "  SELECT num AS value FROM TABLE(sequence(0, 10, 1)) AS t(num) " +
                        "  UNION ALL " +
                        "  SELECT num AS value FROM TABLE(sequence(15, 25, 1)) AS t(num)" +
                        ") s ON t.nationkey = s.value " +
                        "ORDER BY t.name",
                "SELECT name, nationkey FROM tpch.tiny.nation WHERE nationkey <= 10 OR (nationkey >= 15 AND nationkey <= 24) ORDER BY name");
    }

    @Test
    public void testMultipleUnionAllBranches()
    {
        // Test complex UNION ALL with 4 branches
        assertQuery("SELECT type, COUNT(*) AS cnt " +
                        "FROM (" +
                        "  SELECT 'A' AS type, num FROM TABLE(sequence(0, 50, 1)) AS t(num) " +
                        "  UNION ALL " +
                        "  SELECT 'B' AS type, num FROM TABLE(sequence(0, 30, 1)) AS t(num) " +
                        "  UNION ALL " +
                        "  SELECT 'C' AS type, num FROM TABLE(sequence(0, 40, 1)) AS t(num) " +
                        "  UNION ALL " +
                        "  SELECT 'D' AS type, num FROM TABLE(sequence(0, 20, 1)) AS t(num)" +
                        ") " +
                        "GROUP BY type " +
                        "ORDER BY type",
                "VALUES ('A', BIGINT '51'), ('B', BIGINT '31'), ('C', BIGINT '41'), ('D', BIGINT '21')");
    }
}
