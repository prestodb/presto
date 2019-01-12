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

package com.facebook.presto.hive.security;

import com.facebook.presto.plugin.base.security.ForwardingConnectorAccessControl;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorAccessControl;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.security.AccessDeniedException;
import com.facebook.presto.spi.security.Identity;

import java.util.Set;

import static com.facebook.presto.hive.HiveMetadata.getSourceTableNameForPartitionsTable;
import static com.facebook.presto.hive.HiveMetadata.isPartitionsSystemTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denySelectTable;
import static java.util.Objects.requireNonNull;

public class PartitionsAwareAccessControl
        extends ForwardingConnectorAccessControl
{
    private final ConnectorAccessControl delegate;

    public PartitionsAwareAccessControl(ConnectorAccessControl delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    protected ConnectorAccessControl delegate()
    {
        return delegate;
    }

    @Override
    public void checkCanSelectFromColumns(ConnectorTransactionHandle transactionHandle, Identity identity, SchemaTableName tableName, Set<String> columnNames)
    {
        if (isPartitionsSystemTable(tableName)) {
            try {
                checkCanSelectFromColumns(transactionHandle, identity, getSourceTableNameForPartitionsTable(tableName), columnNames);
                return;
            }
            catch (AccessDeniedException e) {
                denySelectTable(tableName.toString());
            }
        }

        delegate.checkCanSelectFromColumns(transactionHandle, identity, tableName, columnNames);
    }
}
