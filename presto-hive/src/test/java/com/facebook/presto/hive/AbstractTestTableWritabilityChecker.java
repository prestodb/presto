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

import com.facebook.presto.hive.metastore.PrestoTableType;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.PrestoException;
import org.apache.hadoop.hive.metastore.ProtectMode;
import org.testng.annotations.Test;

import static com.facebook.presto.hive.HiveStorageFormat.ORC;
import static com.facebook.presto.hive.metastore.MetastoreUtil.PRESTO_OFFLINE;
import static com.facebook.presto.hive.metastore.PrestoTableType.EXTERNAL_TABLE;
import static com.facebook.presto.hive.metastore.PrestoTableType.MANAGED_TABLE;
import static com.facebook.presto.hive.metastore.PrestoTableType.MATERIALIZED_VIEW;
import static com.facebook.presto.hive.metastore.PrestoTableType.TEMPORARY_TABLE;
import static com.facebook.presto.hive.metastore.StorageFormat.fromHiveStorageFormat;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public abstract class AbstractTestTableWritabilityChecker
{
    private final TableWritabilityChecker writesToNonManagedTablesEnabled;
    private final TableWritabilityChecker writesToNonManagedTablesDisabled;

    protected AbstractTestTableWritabilityChecker(
            TableWritabilityChecker writesToNonManagedTablesEnabled,
            TableWritabilityChecker writesToNonManagedTablesDisabled)
    {
        this.writesToNonManagedTablesEnabled = requireNonNull(writesToNonManagedTablesEnabled, "writesToNonManagedTablesEnabled is null");
        this.writesToNonManagedTablesDisabled = requireNonNull(writesToNonManagedTablesDisabled, "writesToNonManagedTablesDisabled is null");
    }

    @Test
    public void testTableTypeCheck()
    {
        writesToNonManagedTablesEnabled.checkTableWritable(createTableWithType(MANAGED_TABLE));
        writesToNonManagedTablesDisabled.checkTableWritable(createTableWithType(MANAGED_TABLE));
        writesToNonManagedTablesEnabled.checkTableWritable(createTableWithType(MATERIALIZED_VIEW));
        writesToNonManagedTablesDisabled.checkTableWritable(createTableWithType(MATERIALIZED_VIEW));
        writesToNonManagedTablesEnabled.checkTableWritable(createTableWithType(TEMPORARY_TABLE));
        writesToNonManagedTablesDisabled.checkTableWritable(createTableWithType(TEMPORARY_TABLE));
        writesToNonManagedTablesEnabled.checkTableWritable(createTableWithType(EXTERNAL_TABLE));
        assertThatThrownBy(() -> writesToNonManagedTablesDisabled.checkTableWritable(createTableWithType(EXTERNAL_TABLE)))
                .isInstanceOf(PrestoException.class)
                .hasMessage("Cannot write to non-managed Hive table");
    }

    @Test
    public void testProtectedTables()
    {
        assertThatThrownBy(() -> writesToNonManagedTablesDisabled.checkTableWritable(tableBuilder()
                .setTableType(MANAGED_TABLE)
                .setParameter(ProtectMode.PARAMETER_NAME, ProtectMode.FLAG_READ_ONLY)
                .build()))
                .isInstanceOf(HiveReadOnlyException.class);
        assertThatThrownBy(() -> writesToNonManagedTablesDisabled.checkTableWritable(tableBuilder()
                .setTableType(MANAGED_TABLE)
                .setParameter(ProtectMode.PARAMETER_NAME, ProtectMode.FLAG_OFFLINE)
                .build()))
                .isInstanceOf(TableOfflineException.class);
        assertThatThrownBy(() -> writesToNonManagedTablesDisabled.checkTableWritable(tableBuilder()
                .setTableType(MANAGED_TABLE)
                .setParameter(PRESTO_OFFLINE, "true")
                .build()))
                .isInstanceOf(TableOfflineException.class);
    }

    @Test
    public void testSkewedTables()
    {
        assertThatThrownBy(() -> writesToNonManagedTablesDisabled.checkTableWritable(tableBuilder()
                .setTableType(MANAGED_TABLE)
                .withStorage(builder -> builder
                        .setStorageFormat(fromHiveStorageFormat(ORC))
                        .setLocation("file://tmp/location")
                        .setSkewed(true)
                        .build())
                .build()))
                .isInstanceOf(PrestoException.class)
                .hasMessageContaining("Inserting into bucketed tables with skew is not supported");
    }

    private static Table createTableWithType(PrestoTableType tableType)
    {
        return tableBuilder()
                .setTableType(tableType)
                .build();
    }

    private static Table.Builder tableBuilder()
    {
        return Table.builder()
                .setDatabaseName("test_schema")
                .setTableName("test_table")
                .setOwner("test_owner")
                .withStorage(builder -> builder
                        .setStorageFormat(fromHiveStorageFormat(ORC))
                        .setLocation("file://tmp/location")
                        .build());
    }
}
