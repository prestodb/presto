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

import com.facebook.presto.tests.ImmutableTpchTablesRequirements.ImmutableNationTable;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.teradata.tempto.ProductTest;
import com.teradata.tempto.Requires;
import com.teradata.tempto.hadoop.hdfs.HdfsClient;
import org.testng.annotations.Test;

import static com.facebook.presto.tests.TestGroups.HDFS_IMPERSONATION;
import static com.facebook.presto.tests.TestGroups.HDFS_NO_IMPERSONATION;
import static com.facebook.presto.tests.TestGroups.PROFILE_SPECIFIC_TESTS;
import static com.teradata.tempto.query.QueryExecutor.query;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

@Requires(ImmutableNationTable.class)
public class ImpersonationTests
        extends ProductTest
{
    @Inject
    private HdfsClient hdfsClient;

    @Inject
    @Named("databases.presto.jdbc_user")
    private String prestoJdbcUser;

    // The value for configuredHdfsUser is profile dependent
    // For non-Kerberos environments this variable will be equal to -DHADOOP_USER_NAME as set in jvm.config
    // For Kerberized environments this variable will be equal to the hive.hdfs.presto.principal property as set in hive.properties
    @Inject
    @Named("databases.presto.configured_hdfs_user")
    private String configuredHdfsUser;

    @Inject
    @Named("databases.hive.warehouse_directory_path")
    private String warehouseDirectoryPath;

    @Test(groups = {HDFS_NO_IMPERSONATION, PROFILE_SPECIFIC_TESTS})
    public void testHdfsImpersonationDisabled()
            throws Exception
    {
        String tableName = "check_hdfs_impersonation_disabled";
        checkTableOwner(tableName, configuredHdfsUser);
    }

    @Test(groups = {HDFS_IMPERSONATION, PROFILE_SPECIFIC_TESTS})
    public void testHdfsImpersonationEnabled()
            throws Exception
    {
        String tableName = "check_hdfs_impersonation_enabled";
        checkTableOwner(tableName, prestoJdbcUser);
    }

    private String getTableLocation(String tableName)
    {
        return warehouseDirectoryPath + '/' + tableName;
    }

    private void checkTableOwner(String tableName, String expectedOwner)
    {
        query(format("DROP TABLE IF EXISTS %s", tableName));
        query(format("CREATE TABLE %s AS SELECT * FROM NATION", tableName));
        String tableLocation = getTableLocation(tableName);
        String owner = hdfsClient.getOwner(tableLocation);
        assertEquals(owner, expectedOwner);
        query(format("DROP TABLE IF EXISTS %s", tableName));
    }
}
