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
package com.facebook.presto.hive;

import com.facebook.presto.hive.containers.HiveMinIODataLake;
import com.facebook.presto.hive.s3.S3HiveQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Map;

import static com.facebook.airlift.testing.Closeables.closeAllRuntimeException;
import static com.facebook.presto.hive.containers.HiveHadoopContainer.HIVE3_IMAGE;
import static com.facebook.presto.tests.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;

public abstract class AbstractHiveHttpProtocolTest
        extends AbstractTestQueryFramework
{
    private static final String HIVE_TEST_SCHEMA = "hive_http_enabled";
    private String bucketName;
    private HiveMinIODataLake dockerizedS3DataLake;

    private final Map<String, String> httpConfig;

    AbstractHiveHttpProtocolTest() throws URISyntaxException {
        httpConfig = ImmutableMap.<String, String>builder()
                // This is required when connecting to ssl and http enabled metastore
                .put("hive.metastore.http.client.tls.enabled", "true")
                .put("hive.metastore.http.client.tls.keystore-path", Paths.get((TestHiveSslWithTrustStoreKeyStore.class.getResource("/hive_ssl_enable/hive-metastore.jks")).toURI()).toFile().toString())
                .put("hive.metastore.http.client.tls.keystore-password", "123456")
                .put("hive.metastore.http.client.tls.truststore-path", Paths.get((TestHiveSslWithTrustStoreKeyStore.class.getResource("/hive_ssl_enable/hive-metastore-truststore.jks")).toURI()).toFile().toString())
                .put("hive.metastore.http.client.tls.truststore-password", "123456")
                .put("hive.metastore.http.client.authentication.type", "BEARER")
                .put("hive.metastore.http.client.bearer-token", "test_token")
                .build();
    }

    protected QueryRunner createQueryRunner() throws Exception
    {
        this.bucketName = "test-hive-http-enable-" + randomTableSuffix();
        this.dockerizedS3DataLake = new HiveMinIODataLake(bucketName, ImmutableMap.of(), HIVE3_IMAGE, true);
        this.dockerizedS3DataLake.start();
        return S3HiveQueryRunner.create(
                this.dockerizedS3DataLake.getHiveHadoop().getHiveMetastoreEndpoint(),
                this.dockerizedS3DataLake.getMinio().getMinioApiEndpoint(),
                HiveMinIODataLake.ACCESS_KEY,
                HiveMinIODataLake.SECRET_KEY,
                ImmutableMap.<String, String>builder()
                        // This is required when using MinIO which requires path style access
                        .put("hive.s3.path-style-access", "true")
                        .build(), httpConfig);
    }

    @BeforeClass
    public void setUp()
    {
        computeActual(format(
                "CREATE SCHEMA hive.%1$s WITH (location='s3a://%2$s/%1$s')",
                HIVE_TEST_SCHEMA,
                bucketName));
    }

    @AfterClass(alwaysRun = true)
    public void close()
            throws Exception
    {
        closeAllRuntimeException(dockerizedS3DataLake);
    }

    @Test
    public void testInsertTable()
    {
        String testTable = getTestTableName();
        computeActual(getCreateTableStatement(
                testTable));
        computeActual(format("INSERT INTO %s values(1, 'TestName1')", testTable));
        computeActual(format("INSERT INTO %s values(2, 'TestName2')", testTable));
        assertQuery(format("SELECT count(*) FROM %s", testTable), "SELECT 2");
        assertQuery(format("SELECT * FROM %s", testTable), "values(1, 'TestName1'), (2, 'TestName2')");
        assertUpdate(format("DROP TABLE %s", testTable));
    }

    protected String getCreateTableStatement(String testTableName)
    {
        return format(
                "CREATE TABLE %s (" +
                        "    id int, " +
                        "    name varchar(25))",
                testTableName);
    }

    protected String getTestTableName()
    {
        return format("hive.%s.%s", HIVE_TEST_SCHEMA, "http_" + randomTableSuffix());
    }
}
