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
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public abstract class AbstractTestApproximateQueries
        extends AbstractTestQueries
{
    private final Optional<Session> sampledSession;

    protected AbstractTestApproximateQueries(QueryRunner queryRunner)
    {
        this(queryRunner, Optional.empty());
    }

    protected AbstractTestApproximateQueries(QueryRunner queryRunner, Session sampledSession)
    {
        this(queryRunner, Optional.of(requireNonNull(sampledSession, "sampledSession is null")));
    }

    private AbstractTestApproximateQueries(QueryRunner queryRunner, Optional<Session> sampledSession)
    {
        super(queryRunner);
        this.sampledSession = requireNonNull(sampledSession, "sampledSession is null");
    }

    @Test
    public void testApproximateQueryCount()
            throws Exception
    {
        assertApproximateQuery("SELECT COUNT(*) FROM orders APPROXIMATE AT 99.999 CONFIDENCE", "SELECT 2 * COUNT(*) FROM orders");
    }

    @Test
    public void testApproximateJoin()
            throws Exception
    {
        assertApproximateQuery(
                "SELECT COUNT(clerk) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey APPROXIMATE AT 99.999 CONFIDENCE",
                "SELECT 4 * COUNT(clerk) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey");
    }

    @Test
    public void testApproximateQueryCountCustkey()
            throws Exception
    {
        assertApproximateQuery("SELECT COUNT(custkey) FROM orders APPROXIMATE AT 99.999 CONFIDENCE", "SELECT 2 * COUNT(custkey) FROM orders");
    }

    @Test
    public void testApproximateQuerySum()
            throws Exception
    {
        assertApproximateQuery("SELECT SUM(totalprice) FROM orders APPROXIMATE AT 99.999 CONFIDENCE", "SELECT 2 * SUM(totalprice) FROM orders");
    }

    @Test
    public void testApproximateQueryAverage()
            throws Exception
    {
        assertApproximateQuery("SELECT AVG(totalprice) FROM orders APPROXIMATE AT 99.999 CONFIDENCE", "SELECT AVG(totalprice) FROM orders");
    }

    protected void assertApproximateQuery(@Language("SQL") String actual, @Language("SQL") String expected)
            throws Exception
    {
        if (sampledSession.isPresent()) {
            assertApproximateQuery(sampledSession.get(), actual, expected);
        }
    }
}
