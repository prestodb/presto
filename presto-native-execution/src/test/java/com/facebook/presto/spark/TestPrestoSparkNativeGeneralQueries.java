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

import com.facebook.presto.nativeworker.AbstractTestNativeGeneralQueries;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import org.testng.annotations.Ignore;

public class TestPrestoSparkNativeGeneralQueries
        extends AbstractTestNativeGeneralQueries
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

    // TODO: Enable following Ignored tests after fixing (Tests can be enabled by removing the method)
    @Override
    @Ignore
    public void testCatalogWithCacheEnabled() {}

    @Override
    @Ignore
    public void testInformationSchemaTables() {}

    @Override
    @Ignore
    public void testShowAndDescribe() {}

    // @TODO Refer https://github.com/prestodb/presto/issues/20294
    @Override
    @Ignore
    public void testAnalyzeStats() {}
}
