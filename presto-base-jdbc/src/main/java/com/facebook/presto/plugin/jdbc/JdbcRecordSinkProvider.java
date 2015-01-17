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

import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorRecordSinkProvider;
import com.facebook.presto.spi.RecordSink;

import javax.inject.Inject;

import static com.facebook.presto.plugin.jdbc.Types.checkType;
import static com.google.common.base.Preconditions.checkNotNull;

public class JdbcRecordSinkProvider
        implements ConnectorRecordSinkProvider
{
    private final JdbcClient jdbcClient;

    @Inject
    public JdbcRecordSinkProvider(JdbcClient jdbcClient)
    {
        this.jdbcClient = checkNotNull(jdbcClient, "jdbcClient is null");
    }

    @Override
    public RecordSink getRecordSink(ConnectorOutputTableHandle tableHandle)
    {
        return new JdbcRecordSink(checkType(tableHandle, JdbcOutputTableHandle.class, "tableHandle"), jdbcClient);
    }

    @Override
    public RecordSink getRecordSink(ConnectorInsertTableHandle tableHandle)
    {
        throw new UnsupportedOperationException();
    }
}
