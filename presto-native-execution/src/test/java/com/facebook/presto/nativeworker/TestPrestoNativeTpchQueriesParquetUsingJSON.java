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
package com.facebook.presto.nativeworker;

import com.facebook.presto.Session;
import com.facebook.presto.iceberg.FileFormat;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.DistributedQueryRunner;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.nio.file.Paths;

@Test(groups = {"parquet"})
public class TestPrestoNativeTpchQueriesParquetUsingJSON
        extends AbstractTestNativeTpchQueries
{
    @Override
    protected QueryRunner createQueryRunner() throws Exception
    {
        QueryRunner queryRunner = PrestoNativeQueryRunnerUtils.createNativeQueryRunner(false, "PARQUET");
        Session session = queryRunner.getDefaultSession();
        System.out.println("--[TestPrestoNativeTpchQueriesParquetUsingJSON]-Default session from actual query runner: " + session.getCatalog() + "--" + session.getSchema());
        // default addStorageFormatToPath is true
        Path hiveDataDirectory = Paths.get(((DistributedQueryRunner) queryRunner).getCoordinator().getDataDirectory() + "/" + FileFormat.PARQUET.name());
        System.out.println("--[TestPrestoNativeTpchQueriesParquetUsingJSON]--actual query runner data directory: " + hiveDataDirectory.toString());
        return queryRunner;
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner() throws Exception
    {
        QueryRunner queryRunner = PrestoNativeQueryRunnerUtils.createJavaQueryRunner("PARQUET");
        Session session = queryRunner.getDefaultSession();
        System.out.println("--[TestPrestoNativeTpchQueriesParquetUsingJSON]-Default session from expected query runner: " + session.getCatalog() + "--" + session.getSchema());
        // default addStorageFormatToPath is true
        Path hiveDataDirectory = Paths.get(((DistributedQueryRunner) queryRunner).getCoordinator().getDataDirectory() + "/" + FileFormat.PARQUET.name());
        System.out.println("--[TestPrestoNativeTpchQueriesParquetUsingJSON]--expected query runner data directory: " + hiveDataDirectory.toString());
        return queryRunner;
    }
}
