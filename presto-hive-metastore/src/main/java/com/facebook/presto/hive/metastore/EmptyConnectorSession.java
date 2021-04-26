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

import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.function.SqlFunctionId;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.facebook.presto.spi.security.ConnectorIdentity;

import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * This is a basic connector session. Should only be used to create HdfsContext, in the absence of FullConnectorSession.
 */

public class EmptyConnectorSession
        implements ConnectorSession
{
    private static final String NO_QUERY_ID = "";
    private final ConnectorIdentity connectorIdentity;

    public EmptyConnectorSession(ConnectorIdentity connectorIdentity)
    {
        this.connectorIdentity = connectorIdentity;
    }

    @Override
    public String getQueryId()
    {
        return NO_QUERY_ID;
    }

    @Override
    public Optional<String> getSource()
    {
        return Optional.empty();
    }

    @Override
    public ConnectorIdentity getIdentity()
    {
        return connectorIdentity;
    }

    @Override
    public Locale getLocale()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<String> getTraceToken()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<String> getClientInfo()
    {
        return Optional.empty();
    }

    @Override
    public long getStartTime()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public SqlFunctionProperties getSqlFunctionProperties()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<SqlFunctionId, SqlInvokedFunction> getSessionFunctions()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T getProperty(String name, Class<T> type)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<String> getSchema()
    {
        throw new UnsupportedOperationException();
    }
}
