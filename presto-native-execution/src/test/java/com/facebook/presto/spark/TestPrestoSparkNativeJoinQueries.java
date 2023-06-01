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
package com.facebook.presto.spark;

import com.facebook.presto.nativeworker.AbstractTestNativeJoinQueries;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

public class TestPrestoSparkNativeJoinQueries
        extends AbstractTestNativeJoinQueries
{
    @Override
    protected QueryRunner createQueryRunner()
    {
        return PrestoSparkNativeQueryRunnerUtils.createHiveRunner();
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return PrestoSparkNativeQueryRunnerUtils.createJavaQueryRunner();
    }

    @Override
    protected void assertQuery(String sql)
    {
        super.assertQuery(sql);
        PrestoSparkNativeQueryRunnerUtils.assertShuffleMetadata();
    }

    @Override
    public Object[][] joinTypeProviderImpl()
    {
        return new Object[][] {{partitionedJoin()}};
    }

    @Test
    public void testBroadcastJoin()
    {
        assertQueryFails(broadcastJoin(), "SELECT * FROM orders o, lineitem l WHERE o.orderkey = l.orderkey",
                ".*Broadcast shuffle is not supported");
    }

    // TODO: Enable following Ignored tests after fixing (Tests can be enabled by removing the method)
    // Cross join requires broadcast join
    @Override
    @Ignore
    public void testCrossJoin() {}
}
