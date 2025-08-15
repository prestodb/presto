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

import javax.inject.Inject;

import java.util.Optional;

import static com.facebook.presto.hive.HiveWriteUtils.checkWritable;
import static com.facebook.presto.hive.metastore.MetastoreUtil.getProtectMode;
import static com.facebook.presto.hive.metastore.PrestoTableType.MANAGED_TABLE;
import static com.facebook.presto.hive.metastore.PrestoTableType.MATERIALIZED_VIEW;
import static com.facebook.presto.hive.metastore.PrestoTableType.TEMPORARY_TABLE;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

public class HiveTableWritabilityChecker
        implements TableWritabilityChecker
{
    private final boolean writesToNonManagedTablesEnabled;

    @Inject
    public HiveTableWritabilityChecker(HiveClientConfig hiveClientConfig)
    {
        this(requireNonNull(hiveClientConfig, "hiveClientConfig is null").getWritesToNonManagedTablesEnabled());
    }

    public HiveTableWritabilityChecker(boolean writesToNonManagedTablesEnabled)
    {
        this.writesToNonManagedTablesEnabled = writesToNonManagedTablesEnabled;
    }

    @Override
    public void checkTableWritable(Table table)
    {
        PrestoTableType tableType = table.getTableType();
        if (!writesToNonManagedTablesEnabled
                && !tableType.equals(MANAGED_TABLE)
                && !tableType.equals(MATERIALIZED_VIEW)
                && !tableType.equals(TEMPORARY_TABLE)) {
            throw new PrestoException(NOT_SUPPORTED, "Cannot write to non-managed Hive table");
        }

        checkWritable(
                table.getSchemaTableName(),
                Optional.empty(),
                getProtectMode(table),
                table.getParameters(),
                table.getStorage());
    }
}
