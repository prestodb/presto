/*
 * Copyright 2016 Bloomberg L.P.
 *
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
package com.facebook.presto.accumulo;

import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.charset.Charset;

import static com.facebook.presto.accumulo.AccumuloQueryRunner.createAccumuloQueryRunner;
import static com.facebook.presto.accumulo.AccumuloQueryRunner.dropTpchTables;

public class TestAccumuloTpchQueries
{
    private DistributedQueryRunner runner;
    private Charset charset = Charset.forName("UTF-8");

    public TestAccumuloTpchQueries()
            throws Exception
    {
        runner = createAccumuloQueryRunner(ImmutableMap.of(), true);
    }

    @AfterClass
    public void cleanup()
    {
        dropTpchTables(runner, runner.getDefaultSession());
    }

    @Test
    public void testQuery1()
            throws Exception
    {
        runQuery("src/test/resources/tpch/1.sql");
    }

    @Test
    public void testQuery2()
            throws Exception
    {
        runQuery("src/test/resources/tpch/2.sql");
    }

    @Test
    public void testQuery3()
            throws Exception
    {
        runQuery("src/test/resources/tpch/3.sql");
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testQuery4()
            throws Exception
    {
        // not yet implemented: com.facebook.presto.sql.tree.ExistsPredicate
        runQuery("src/test/resources/tpch/4.sql");
    }

    @Test
    public void testQuery5()
            throws Exception
    {
        runQuery("src/test/resources/tpch/5.sql");
    }

    @Test
    public void testQuery6()
            throws Exception
    {
        runQuery("src/test/resources/tpch/6.sql");
    }

    @Test
    public void testQuery7()
            throws Exception
    {
        runQuery("src/test/resources/tpch/7.sql");
    }

    @Test
    public void testQuery8()
            throws Exception
    {
        runQuery("src/test/resources/tpch/8.sql");
    }

    @Test
    public void testQuery9()
            throws Exception
    {
        runQuery("src/test/resources/tpch/9.sql");
    }

    @Test
    public void testQuery10()
            throws Exception
    {
        runQuery("src/test/resources/tpch/10.sql");
    }

    @Test
    public void testQuery11()
            throws Exception
    {
        runQuery("src/test/resources/tpch/11.sql");
    }

    @Test
    public void testQuery12()
            throws Exception
    {
        runQuery("src/test/resources/tpch/12.sql");
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testQuery13()
            throws Exception
    {
        // Non-equi joins only supported for inner join: (NOT ("o"."comment" LIKE '%special%requests%'))
        runQuery("src/test/resources/tpch/13.sql");
    }

    @Test
    public void testQuery14()
            throws Exception
    {
        runQuery("src/test/resources/tpch/14.sql");
    }

    @Test
    public void testQuery15()
            throws Exception
    {
        runner.execute("create view revenue as " +
                "select " +
                "l.suppkey AS number, " +
                "sum(l.extendedprice * (1 - l.discount)) AS revenue " +
                "from " +
                "lineitem l " +
                "where " +
                "l.shipdate >= date '1996-01-01' " +
                "and l.shipdate < date '1996-01-01' + interval '3' month " +
                "group by " +
                "l.suppkey");

        runQuery("src/test/resources/tpch/15.sql");

        runner.execute("DROP VIEW revenue");
    }

    @Test
    public void testQuery16()
            throws Exception
    {
        runQuery("src/test/resources/tpch/16.sql");
    }

    @Test
    public void testQuery17()
            throws Exception
    {
        runQuery("src/test/resources/tpch/17.sql");
    }

    @Test
    public void testQuery18()
            throws Exception
    {
        runQuery("src/test/resources/tpch/18.sql");
    }

    @Test
    public void testQuery19()
            throws Exception
    {
        runQuery("src/test/resources/tpch/19.sql");
    }

    @Test
    public void testQuery20()
            throws Exception
    {
        runQuery("src/test/resources/tpch/20.sql");
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testQuery21()
            throws Exception
    {
        // not yet implemented: com.facebook.presto.sql.tree.ExistsPredicate
        runQuery("src/test/resources/tpch/21.sql");
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testQuery22()
            throws Exception
    {
        // not yet implemented: com.facebook.presto.sql.tree.ExistsPredicate
        runQuery("src/test/resources/tpch/22.sql");
    }

    private void runQuery(String file)
            throws Exception
    {
        runner.execute(Files.toString(new File(file), charset));
    }
}
