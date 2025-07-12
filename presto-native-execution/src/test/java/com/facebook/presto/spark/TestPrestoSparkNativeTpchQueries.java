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

import com.facebook.presto.nativeworker.AbstractTestNativeTpchQueries;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import org.testng.annotations.Ignore;

public class TestPrestoSparkNativeTpchQueries
        extends AbstractTestNativeTpchQueries
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

    // TODO: Enable following Ignored tests after fixing (Tests can be enabled by removing the method)
    // Following tests require broadcast join
    @Override
    @Ignore
    public void testTpchQ7() {}

    @Override
    @Ignore
    public void testTpchQ8() {}

    @Override
    @Ignore
    public void testTpchQ11() {}

    @Override
    @Ignore
    public void testTpchQ15() {}

    @Override
    @Ignore
    public void testTpchQ18() {}

    @Override
    @Ignore
    public void testTpchQ21() {}

    @Override
    @Ignore
    public void testTpchQ22() {}
}
