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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.testing.QueryRunner;
import org.testng.annotations.Test;

import static com.google.common.base.Preconditions.checkState;

public abstract class AbstractTestApproximateQueries
        extends AbstractTestQueries
{
    private final ConnectorSession defaultSampledSession;

    protected AbstractTestApproximateQueries(QueryRunner queryRunner, ConnectorSession defaultSampledSession)
    {
        super(queryRunner);
        this.defaultSampledSession = defaultSampledSession;
    }

    @Test
    public void testApproximateQueryCount()
            throws Exception
    {
        checkState(defaultSampledSession != null, "no defaultSampledSession found");
        assertApproximateQuery(defaultSampledSession, "SELECT COUNT(*) FROM orders APPROXIMATE AT 99.999 CONFIDENCE", "SELECT 2 * COUNT(*) FROM orders");
    }

    @Test
    public void testApproximateQueryCountCustkey()
            throws Exception
    {
        checkState(defaultSampledSession != null, "no defaultSampledSession found");
        assertApproximateQuery(defaultSampledSession, "SELECT COUNT(custkey) FROM orders APPROXIMATE AT 99.999 CONFIDENCE", "SELECT 2 * COUNT(custkey) FROM orders");
    }

    @Test
    public void testApproximateQuerySum()
            throws Exception
    {
        checkState(defaultSampledSession != null, "no defaultSampledSession found");
        assertApproximateQuery(defaultSampledSession, "SELECT SUM(totalprice) FROM orders APPROXIMATE AT 99.999 CONFIDENCE", "SELECT 2 * SUM(totalprice) FROM orders");
    }

    @Test
    public void testApproximateQueryAverage()
            throws Exception
    {
        checkState(defaultSampledSession != null, "no defaultSampledSession found");
        assertApproximateQuery(defaultSampledSession, "SELECT AVG(totalprice) FROM orders APPROXIMATE AT 99.999 CONFIDENCE", "SELECT AVG(totalprice) FROM orders");
    }
}
