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

import com.facebook.airlift.log.Level;
import com.facebook.airlift.log.Logging;
import com.facebook.presto.nativeworker.AbstractTestNativeGeneralQueries;
import com.facebook.presto.scalar.sql.SqlInvokedFunctionsPlugin;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import org.testng.annotations.Ignore;

import java.util.ArrayList;
import java.util.List;

public class TestPrestoSparkNativeGeneralQueries
        extends AbstractTestNativeGeneralQueries
{
    private static final Logging logging = Logging.initialize();

    @Override
    protected QueryRunner createQueryRunner()
    {
        QueryRunner queryRunner = PrestoSparkNativeQueryRunnerUtils.createHiveRunner();

        // Install plugins needed for extra array functions.
        queryRunner.installPlugin(new SqlInvokedFunctionsPlugin());
        return queryRunner;
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = PrestoSparkNativeQueryRunnerUtils.createJavaQueryRunner();

        // Install plugins needed for extra array functions.
        queryRunner.installPlugin(new SqlInvokedFunctionsPlugin());
        return queryRunner;
    }

    @Override
    public void testUnicodeInJson()
    {
        // Suppress the logging for large volume of query text to avoid console hangs.
        List<String> suppressLogList = new ArrayList<>();
        suppressLogList.add("com.facebook.presto.spark.execution.PrestoSparkStaticQueryExecution");
        suppressLogList.add("com.facebook.presto.spark.PrestoSparkQueryExecutionFactory");
        try {
            for (String logLocation : suppressLogList) {
                logging.setLevel(logLocation, Level.WARN);
            }
            super.testUnicodeInJson();
        }
        finally {
            for (String logLocation : suppressLogList) {
                logging.setLevel(logLocation, Level.INFO);
            }
        }
    }

    // Disable: Only applicable for Presto-Native single node mode, not applicable for POS.
    @Override
    @Ignore
    public void testDistributedSortSingleNode() {}

    // Disable: Text file reader is not supported. This test is also disabled in pom.xml through disabling groups "textfile_reader".
    @Override
    public void testReadTableWithTextfileFormat() {}

    // Disable: Not supporte by POS
    @Override
    @Ignore
    public void testInformationSchemaTables() {}

    // Disable: Not supporte by POS
    @Override
    @Ignore
    public void testShowAndDescribe() {}

    // Disable: Not supporte by POS
    @Override
    @Ignore
    public void testSystemTables() {}

    // Disable: Not supporte by POS
    @Override
    @Ignore
    public void testShowSessionWithoutJavaSessionProperties() {}

    // Disable: Not supporte by POS
    @Override
    @Ignore
    public void testSetSessionJavaWorkerSessionProperty() {}

    // Disable: PrestoSparkQueryRunner does not support pattern assertion.
    @Override
    @Ignore
    public void testRowWiseExchange() {}

    // TODO: Enable following Ignored tests after fixing (Tests can be enabled by removing the method)

    // This test is broken likely due to Parquet related issues.
    @Override
    @Ignore
    public void testAnalyzeStatsOnDecimals() {}

    // VeloxRuntimeError: it != connectors().end() Connector with ID 'hivecached' not registered
    @Override
    @Ignore
    public void testCatalogWithCacheEnabled() {}
}
