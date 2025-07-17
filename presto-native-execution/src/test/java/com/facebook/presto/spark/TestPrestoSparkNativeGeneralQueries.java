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
        return PrestoSparkNativeQueryRunnerUtils.createHiveRunner();
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return PrestoSparkNativeQueryRunnerUtils.createJavaQueryRunner();
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

    // TODO: Enable following Ignored tests after fixing (Tests can be enabled by removing the method)
    @Override
    @Ignore
    public void testAnalyzeStatsOnDecimals() {}

    // VeloxUserError:  Unsupported file format in TableWrite: "ORC".
    @Override
    @Ignore
    public void testColumnFilter() {}

    // VeloxUserError:  Unsupported file format in TableWrite: "ORC".
    @Override
    @Ignore
    public void testIPAddressIPPrefix() {}

    // VeloxUserError:  Unsupported file format in TableWrite: "ORC".
    @Override
    @Ignore
    public void testInvalidUuid() {}

    // VeloxUserError:  Unsupported file format in TableWrite: "ORC".
    @Override
    @Ignore
    public void testStringFunctions() {}

    // VeloxUserError:  Unsupported file format in TableWrite: "ORC".
    @Override
    @Ignore
    public void testUuid() {}

    // Access Denied: Cannot set catalog session property
    // hive.parquet_pushdown_filter_enabled
    @Override
    @Ignore
    public void testDecimalApproximateAggregates() {}

    // Access Denied: Cannot set catalog session property
    // hive.parquet_pushdown_filter_enabled
    @Override
    @Ignore
    public void testDecimalRangeFilters() {}

    // Access Denied: Cannot set catalog session property
    // hive.pushdown_filter_enabled
    @Override
    @Ignore
    public void testTimestampWithTimeZone() {}

    @Override
    @Ignore
    public void testDistributedSortSingleNode() {}

    //VeloxRuntimeError: ReaderFactory is not registered for format text
    @Override
    @Ignore
    public void testReadTableWithTextfileFormat() {}

    @Override
    @Ignore
    public void testInformationSchemaTables() {}

    @Override
    @Ignore
    public void testShowAndDescribe() {}

    @Override
    public void testSystemTables() {}

    // @TODO Refer https://github.com/prestodb/presto/issues/20294
    @Override
    @Ignore
    public void testAnalyzeStats() {}

    // https://github.com/prestodb/presto/issues/22275
    @Override
    @Ignore
    public void testUnionAllInsert() {}

    @Override
    @Ignore
    public void testShowSessionWithoutJavaSessionProperties() {}

    @Override
    @Ignore
    public void testSetSessionJavaWorkerSessionProperty() {}

    @Override
    @Ignore
    public void testRowWiseExchange() {}
}
