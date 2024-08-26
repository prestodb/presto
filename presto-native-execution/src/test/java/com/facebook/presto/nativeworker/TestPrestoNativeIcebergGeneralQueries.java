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
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.file.Path;
import java.util.Optional;

import static com.facebook.presto.iceberg.CatalogType.HIVE;
import static com.facebook.presto.iceberg.IcebergQueryRunner.getIcebergDataDirectoryPath;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.getNativeQueryRunnerParameters;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

public class TestPrestoNativeIcebergGeneralQueries
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        boolean addStorageFormatToPath = false;
        Optional<Path> baseDataDirectory = Optional.of(new File(getNativeQueryRunnerParameters().dataDirectory.toString(), "temp_iceberg").toPath());
        String prestoServerPath = getNativeQueryRunnerParameters().serverBinary.toString();
        Optional<Integer> workerCount = getNativeQueryRunnerParameters().workerCount;
        QueryRunner queryRunner = PrestoNativeQueryRunnerUtils.createNativeIcebergQueryRunner(baseDataDirectory, prestoServerPath, workerCount, 0, false, Optional.empty(), "PARQUET", addStorageFormatToPath);
        Session session = queryRunner.getDefaultSession();
        System.out.println("--[TestPrestoNativeIcebergGeneralQueries]-Default session from actual query runner: " + session.getCatalog() + "--" + session.getSchema());
        Path icebergDataDirectory = getIcebergDataDirectoryPath(((DistributedQueryRunner) queryRunner).getCoordinator().getDataDirectory(), HIVE.name(), FileFormat.PARQUET, addStorageFormatToPath);
        System.out.println("--[TestPrestoNativeIcebergGeneralQueries]--actual query runner data directory: " + icebergDataDirectory.toString());
        return queryRunner;
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        boolean addStorageFormatToPath = false;
        QueryRunner queryRunner = PrestoNativeQueryRunnerUtils.createJavaIcebergQueryRunner(Optional.of(new File(getNativeQueryRunnerParameters().dataDirectory.toString(), "temp_iceberg").toPath()), "PARQUET", addStorageFormatToPath);
        Session session = queryRunner.getDefaultSession();
        System.out.println("--[TestPrestoNativeIcebergGeneralQueries]-Default session from actual query runner: " + session.getCatalog() + "--" + session.getSchema());
        Path icebergDataDirectory = getIcebergDataDirectoryPath(((DistributedQueryRunner) queryRunner).getCoordinator().getDataDirectory(), HIVE.name(), FileFormat.PARQUET, addStorageFormatToPath);
        System.out.println("--[TestPrestoNativeIcebergGeneralQueries]--expected query runner data directory: " + icebergDataDirectory.toString());
        return queryRunner;
    }

    @Override
    protected void createTables()
    {
        createTableToTestHiddenColumns();
    }

    private void createTableToTestHiddenColumns()
    {
        QueryRunner javaQueryRunner = ((QueryRunner) getExpectedQueryRunner());
        if (!javaQueryRunner.tableExists(getSession(), "test_hidden_columns")) {
            javaQueryRunner.execute("CREATE TABLE test_hidden_columns AS SELECT * FROM tpch.tiny.region WHERE regionkey=0");
            javaQueryRunner.execute("INSERT INTO test_hidden_columns SELECT * FROM tpch.tiny.region WHERE regionkey=1");
        }
    }

    @Test
    public void testPathHiddenColumn()
    {
        assertQuery("SELECT \"$path\", * FROM test_hidden_columns");

        // Fetch one of the file paths and use it in a filter
        String filePath = (String) computeActual("SELECT \"$path\" from test_hidden_columns LIMIT 1").getOnlyValue();
        assertQuery(format("SELECT * from test_hidden_columns WHERE \"$path\"='%s'", filePath));

        assertEquals(
                (Long) computeActual(format("SELECT count(*) from test_hidden_columns WHERE \"$path\"='%s'", filePath))
                        .getOnlyValue(),
                1L);

        // Filter for $path that doesn't exist.
        assertEquals(
                (Long) computeActual(format("SELECT count(*) from test_hidden_columns WHERE \"$path\"='%s'", "non-existent-path"))
                        .getOnlyValue(),
                0L);
    }

    @Test
    public void testDataSequenceNumberHiddenColumn()
    {
        assertQuery("SELECT \"$data_sequence_number\", * FROM test_hidden_columns");

        // Fetch one of the data sequence numbers and use it in a filter
        Long dataSequenceNumber = (Long) computeActual("SELECT \"$data_sequence_number\" from test_hidden_columns LIMIT 1").getOnlyValue();
        assertQuery(format("SELECT * from test_hidden_columns WHERE \"$data_sequence_number\"=%d", dataSequenceNumber));

        assertEquals(
                (Long) computeActual(format("SELECT count(*) from test_hidden_columns WHERE \"$data_sequence_number\"=%d", dataSequenceNumber))
                        .getOnlyValue(),
                1L);

        // Filter for $data_sequence_number that doesn't exist.
        assertEquals(
                (Long) computeActual(format("SELECT count(*) from test_hidden_columns WHERE \"$data_sequence_number\"=%d", 1000))
                        .getOnlyValue(),
                0L);
    }
}
