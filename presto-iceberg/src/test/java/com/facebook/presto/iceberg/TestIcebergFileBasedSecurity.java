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
package com.facebook.presto.iceberg;

import com.facebook.presto.Session;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.security.ViewExpression;
import com.facebook.presto.sql.query.QueryAssertions;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.testing.TestingAccessControlManager;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.iceberg.CatalogType.HIVE;
import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.iceberg.IcebergQueryRunner.getIcebergDataDirectoryPath;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.ThrowingRunnable;

@Test(singleThreaded = true)
public class TestIcebergFileBasedSecurity
        extends AbstractTestQueryFramework
{
    private QueryAssertions assertions;
    private TestingAccessControlManager accessControl;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog(ICEBERG_CATALOG)
                .build();
        String path = this.getClass().getResource("security.json").getPath();
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .build();

        Path dataDirectory = queryRunner.getCoordinator().getDataDirectory();
        Path catalogDirectory = getIcebergDataDirectoryPath(dataDirectory, HIVE.name(), new IcebergConfig().getFileFormat(), false);

        queryRunner.installPlugin(new IcebergPlugin());
        Map<String, String> icebergProperties = ImmutableMap.<String, String>builder()
                .put("hive.metastore", "file")
                .put("hive.metastore.catalog.dir", catalogDirectory.toFile().toURI().toString())
                .put("iceberg.security", "file")
                .put("security.config-file", path)
                .build();

        queryRunner.createCatalog(ICEBERG_CATALOG, "iceberg", icebergProperties);

        assertions = new QueryAssertions(queryRunner);
        accessControl = assertions.getQueryRunner().getAccessControl();

        return queryRunner;
    }

    @Test
    public void testCallNormalProcedures()
    {
        Session icebergAdmin = Session.builder(getSession("iceberg"))
                .build();
        Session alice = Session.builder(getSession("alice"))
                .build();
        Session bob = Session.builder(getSession("bob"))
                .build();
        Session joe = Session.builder(getSession("joe"))
                .build();

        // `icebergAdmin`, `alice`, and `bob` have the permission to execute `iceberg.system.invalidate_statistics_file_cache`
        assertUpdate(icebergAdmin, "call system.invalidate_statistics_file_cache()");
        assertUpdate(alice, "call system.invalidate_statistics_file_cache()");
        assertUpdate(bob, "call system.invalidate_statistics_file_cache()");
        // `joe` do not have the permission to execute `iceberg.system.invalidate_statistics_file_cache`
        assertDenied(() -> assertUpdate(joe, "call iceberg.system.invalidate_statistics_file_cache()"),
                "Access Denied: Cannot call procedure system.invalidate_statistics_file_cache");
    }

    @Test
    public void testCallDistributedProceduresWithInsertDeletePermission()
    {
        Session icebergAdmin = Session.builder(getSession("iceberg"))
                .build();
        Session alice = Session.builder(getSession("alice"))
                .build();
        Session bob = Session.builder(getSession("bob"))
                .build();
        Session joe = Session.builder(getSession("joe"))
                .build();

        String schema = getSession().getSchema().get();
        String tableName = "test_rewrite_table";

        try {
            assertUpdate(icebergAdmin, "create schema if not exists " + schema);
            assertUpdate(icebergAdmin, "create table " + tableName + " (a int, b varchar)");
            assertUpdate(icebergAdmin, "insert into " + tableName + " values(1, '1001')", 1);
            assertUpdate(icebergAdmin, "insert into " + tableName + " values(2, '1002')", 1);

            // `icebergAdmin` has permission to execute `iceberg.system.rewrite_data_files` and
            // perform INSERT/DELETE operations on the target table involved in the procedure
            assertUpdate(icebergAdmin, format("call system.rewrite_data_files('%s', '%s')", schema, tableName), 2);
            // `alice` and `bob` have the permission to execute `iceberg.system.rewrite_data_files`,
            // but they lack the necessary permission to perform INSERT or DELETE on the target table
            assertDenied(() -> assertUpdate(alice, format("call system.rewrite_data_files('%s', '%s')", schema, tableName)),
                    format("Access Denied: Cannot delete from table %s.%s", schema, tableName));
            assertDenied(() -> assertUpdate(bob, format("call system.rewrite_data_files('%s', '%s')", schema, tableName)),
                    format("Access Denied: Cannot insert into table %s.%s", schema, tableName));
            // `joe` do not have the permission to execute `iceberg.system.rewrite_data_files`
            assertDenied(() -> assertUpdate(joe, format("call system.rewrite_data_files('%s', '%s')", schema, tableName)),
                    "Access Denied: Cannot call procedure system.rewrite_data_files");
        }
        finally {
            assertUpdate(icebergAdmin, "drop table if exists " + tableName);
            assertUpdate(icebergAdmin, "drop schema if exists " + schema);
        }
    }

    @Test
    public void testCallDistributedProceduresWithRowFiltersAndColumnMasks()
    {
        Session icebergAdmin = Session.builder(getSession("iceberg"))
                .build();

        String schema = getSession().getSchema().get();
        String tableName = "test_rewrite_table";

        try {
            assertUpdate(icebergAdmin, "create schema if not exists " + schema);
            assertUpdate(icebergAdmin, "create table " + tableName + " (a int, b varchar)");
            assertUpdate(icebergAdmin, "insert into " + tableName + " values(1, '1001')", 1);
            assertUpdate(icebergAdmin, "insert into " + tableName + " values(2, '1002')", 1);

            // `icebergAdmin` has permission to execute `iceberg.system.rewrite_data_files` and
            // perform INSERT/DELETE operations on the target table involved in the procedure
            assertUpdate(icebergAdmin, format("call system.rewrite_data_files('%s', '%s')", schema, tableName), 2);

            QualifiedObjectName qualifiedTableName = new QualifiedObjectName("iceberg", schema, tableName);
            assertions.executeExclusively(() -> {
                accessControl.reset();
                accessControl.rowFilter(
                        qualifiedTableName,
                        "iceberg",
                        new ViewExpression("iceberg", Optional.empty(), Optional.empty(), "a < 2"));
                assertions.assertQuery(icebergAdmin, "SELECT count(*) FROM " + tableName, "VALUES BIGINT '1'");
                assertions.assertFails(icebergAdmin, format("call system.rewrite_data_files('%s', '%s')", schema, tableName),
                        "Access Denied: Full data access is restricted by row filters and column masks for table: " + qualifiedTableName);
            });

            assertions.executeExclusively(() -> {
                accessControl.reset();
                accessControl.columnMask(qualifiedTableName, "b", "iceberg",
                                new ViewExpression("iceberg", Optional.empty(), Optional.empty(), "'noop'"));
                assertions.assertFails(icebergAdmin, format("call system.rewrite_data_files('%s', '%s')", schema, tableName),
                        "Access Denied: Full data access is restricted by row filters and column masks for table: " + qualifiedTableName);
            });
        }
        finally {
            assertUpdate(icebergAdmin, "drop table if exists " + tableName);
            assertUpdate(icebergAdmin, "drop schema if exists " + schema);
        }
    }

    private Session getSession(String user)
    {
        return testSessionBuilder()
                .setCatalog(getSession().getCatalog().get())
                .setSchema(getSession().getSchema().get())
                .setIdentity(new Identity(user, Optional.empty())).build();
    }

    private static void assertDenied(ThrowingRunnable runnable, String message)
    {
        assertThatThrownBy(runnable::run)
                .isInstanceOf(RuntimeException.class)
                .hasMessageMatching(message);
    }
}
