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
package io.prestosql.plugin.jdbc;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorRecordSetProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.RecordSet;

import javax.inject.Inject;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class JdbcRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private final JdbcClient jdbcClient;

    @Inject
    public JdbcRecordSetProvider(JdbcClient jdbcClient)
    {
        this.jdbcClient = requireNonNull(jdbcClient, "jdbcClient is null");
    }

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorSplit split, List<? extends ColumnHandle> columns)
    {
        JdbcSplit jdbcSplit = (JdbcSplit) split;

        ImmutableList.Builder<JdbcColumnHandle> handles = ImmutableList.builder();
        for (ColumnHandle handle : columns) {
            handles.add((JdbcColumnHandle) handle);
        }

        return new JdbcRecordSet(jdbcClient, session, jdbcSplit, handles.build());
    }
}
