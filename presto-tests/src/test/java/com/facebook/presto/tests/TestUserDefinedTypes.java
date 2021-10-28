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

import com.facebook.presto.Session;
import com.facebook.presto.testing.QueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public class TestUserDefinedTypes
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder().build();
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session).build();
        queryRunner.enableTestFunctionNamespaces(ImmutableList.of("testing"), ImmutableMap.of());
        return queryRunner;
    }

    @Test
    public void testStructType()
    {
        assertQuerySucceeds("CREATE TYPE testing.type.pair AS (fst integer, snd integer)");
        assertQuerySucceeds("CREATE TYPE testing.type.pair3 AS (fst testing.type.pair, snd integer)");
        assertQuery("SELECT p.fst.fst FROM(SELECT CAST(ROW(CAST(ROW(1,2) AS testing.type.pair), 3) AS testing.type.pair3) AS p)", "SELECT 1");
        assertQuerySucceeds("CREATE TYPE testing.type.pair3Alt AS (fst ROW(fst integer, snd integer), snd integer)");
        assertQuery("SELECT p.fst.snd FROM(SELECT CAST(ROW(ROW(1,2), 3) AS testing.type.pair3Alt) AS p)", "SELECT 2");
    }

    @Test
    public void testDistinctType()
    {
        assertQuerySucceeds("CREATE TYPE testing.type.num AS integer");
        assertQuery("SELECT x FROM (SELECT CAST(4 as testing.type.num) AS x)", "SELECT 4");
        assertQuerySucceeds("CREATE TYPE testing.type.mypair AS (fst testing.type.num, snd integer)");
        assertQuery("SELECT p.fst FROM (SELECT CAST(ROW(CAST(4 AS testing.type.num),3) as testing.type.mypair) AS p)", "SELECT 4");
    }
}
