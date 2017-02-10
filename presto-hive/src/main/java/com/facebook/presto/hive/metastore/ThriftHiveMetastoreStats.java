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
package com.facebook.presto.hive.metastore;

import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

public class ThriftHiveMetastoreStats
{
    private final HiveMetastoreApiStats getAllDatabases = new HiveMetastoreApiStats();
    private final HiveMetastoreApiStats getDatabase = new HiveMetastoreApiStats();
    private final HiveMetastoreApiStats getAllTables = new HiveMetastoreApiStats();
    private final HiveMetastoreApiStats getAllViews = new HiveMetastoreApiStats();
    private final HiveMetastoreApiStats getTable = new HiveMetastoreApiStats();
    private final HiveMetastoreApiStats getPartitionNames = new HiveMetastoreApiStats();
    private final HiveMetastoreApiStats getPartitionNamesPs = new HiveMetastoreApiStats();
    private final HiveMetastoreApiStats getPartition = new HiveMetastoreApiStats();
    private final HiveMetastoreApiStats getPartitionsByNames = new HiveMetastoreApiStats();
    private final HiveMetastoreApiStats createDatabase = new HiveMetastoreApiStats();
    private final HiveMetastoreApiStats dropDatabase = new HiveMetastoreApiStats();
    private final HiveMetastoreApiStats alterDatabase = new HiveMetastoreApiStats();
    private final HiveMetastoreApiStats createTable = new HiveMetastoreApiStats();
    private final HiveMetastoreApiStats dropTable = new HiveMetastoreApiStats();
    private final HiveMetastoreApiStats alterTable = new HiveMetastoreApiStats();
    private final HiveMetastoreApiStats addPartitions = new HiveMetastoreApiStats();
    private final HiveMetastoreApiStats dropPartition = new HiveMetastoreApiStats();
    private final HiveMetastoreApiStats alterPartition = new HiveMetastoreApiStats();
    private final HiveMetastoreApiStats loadRoles = new HiveMetastoreApiStats();
    private final HiveMetastoreApiStats getPrivilegeSet = new HiveMetastoreApiStats();
    private final HiveMetastoreApiStats listPrivileges = new HiveMetastoreApiStats();
    private final HiveMetastoreApiStats grantTablePrivileges = new HiveMetastoreApiStats();
    private final HiveMetastoreApiStats revokeTablePrivileges = new HiveMetastoreApiStats();
    private final HiveMetastoreApiStats listRoles = new HiveMetastoreApiStats();
    private final HiveMetastoreApiStats createRole = new HiveMetastoreApiStats();
    private final HiveMetastoreApiStats dropRole = new HiveMetastoreApiStats();

    @Managed
    @Nested
    public HiveMetastoreApiStats getGetAllDatabases()
    {
        return getAllDatabases;
    }

    @Managed
    @Nested
    public HiveMetastoreApiStats getGetDatabase()
    {
        return getDatabase;
    }

    @Managed
    @Nested
    public HiveMetastoreApiStats getGetAllTables()
    {
        return getAllTables;
    }

    @Managed
    @Nested
    public HiveMetastoreApiStats getGetAllViews()
    {
        return getAllViews;
    }

    @Managed
    @Nested
    public HiveMetastoreApiStats getGetTable()
    {
        return getTable;
    }

    @Managed
    @Nested
    public HiveMetastoreApiStats getGetPartitionNames()
    {
        return getPartitionNames;
    }

    @Managed
    @Nested
    public HiveMetastoreApiStats getGetPartitionNamesPs()
    {
        return getPartitionNamesPs;
    }

    @Managed
    @Nested
    public HiveMetastoreApiStats getGetPartition()
    {
        return getPartition;
    }

    @Managed
    @Nested
    public HiveMetastoreApiStats getGetPartitionsByNames()
    {
        return getPartitionsByNames;
    }

    @Managed
    @Nested
    public HiveMetastoreApiStats getCreateDatabase()
    {
        return createDatabase;
    }

    @Managed
    @Nested
    public HiveMetastoreApiStats getDropDatabase()
    {
        return dropDatabase;
    }

    @Managed
    @Nested
    public HiveMetastoreApiStats getAlterDatabase()
    {
        return alterDatabase;
    }

    @Managed
    @Nested
    public HiveMetastoreApiStats getCreateTable()
    {
        return createTable;
    }

    @Managed
    @Nested
    public HiveMetastoreApiStats getDropTable()
    {
        return dropTable;
    }

    @Managed
    @Nested
    public HiveMetastoreApiStats getAlterTable()
    {
        return alterTable;
    }

    @Managed
    @Nested
    public HiveMetastoreApiStats getAddPartitions()
    {
        return addPartitions;
    }

    @Managed
    @Nested
    public HiveMetastoreApiStats getDropPartition()
    {
        return dropPartition;
    }

    @Managed
    @Nested
    public HiveMetastoreApiStats getAlterPartition()
    {
        return alterPartition;
    }

    @Managed
    @Nested
    public HiveMetastoreApiStats getGrantTablePrivileges()
    {
        return grantTablePrivileges;
    }

    @Managed
    @Nested
    public HiveMetastoreApiStats getRevokeTablePrivileges()
    {
        return revokeTablePrivileges;
    }

    @Managed
    @Nested
    public HiveMetastoreApiStats getLoadRoles()
    {
        return loadRoles;
    }

    @Managed
    @Nested
    public HiveMetastoreApiStats getGetPrivilegeSet()
    {
        return getPrivilegeSet;
    }

    @Managed
    @Nested
    public HiveMetastoreApiStats getListPrivileges()
    {
        return listPrivileges;
    }

    @Managed
    @Nested
    public HiveMetastoreApiStats getListRoles()
    {
        return listRoles;
    }

    @Managed
    @Nested
    public HiveMetastoreApiStats getCreateRole()
    {
        return createRole;
    }

    @Managed
    @Nested
    public HiveMetastoreApiStats getDropRole()
    {
        return dropRole;
    }
}
