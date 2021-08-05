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
package com.facebook.presto.plugin.jdbc;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class JdbcRecordSet
        implements RecordSet
{
    private final JdbcClient jdbcClient;
    private final List<JdbcColumnHandle> columnHandles;
    private final List<Type> columnTypes;
    private final JdbcSplit split;
    private final ConnectorSession session;

    public JdbcRecordSet(JdbcClient jdbcClient, ConnectorSession session, JdbcSplit split, List<JdbcColumnHandle> columnHandles)
    {
        this.jdbcClient = requireNonNull(jdbcClient, "jdbcClient is null");
        this.split = requireNonNull(split, "split is null");

        requireNonNull(split, "split is null");
        this.columnHandles = requireNonNull(columnHandles, "column handles is null");
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (JdbcColumnHandle column : columnHandles) {
            types.add(column.getColumnType());
        }
        this.columnTypes = types.build();
        this.session = requireNonNull(session, "session is null");
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return new JdbcRecordCursor(jdbcClient, session, split, columnHandles);
    }
}
