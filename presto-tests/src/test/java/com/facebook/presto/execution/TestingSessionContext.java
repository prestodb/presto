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
package com.facebook.presto.execution;

import com.facebook.presto.Session;
import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.server.SessionContext;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.session.ResourceEstimates;
import com.facebook.presto.transaction.TransactionId;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Map.Entry;
import static java.util.Objects.requireNonNull;

public class TestingSessionContext
        implements SessionContext
{
    private final Session session;

    public TestingSessionContext(Session session)
    {
        this.session = requireNonNull(session, "session is null");
    }

    @Override
    public Identity getIdentity()
    {
        return session.getIdentity();
    }

    @Override
    public String getCatalog()
    {
        return session.getCatalog().orElse(null);
    }

    @Override
    public String getSchema()
    {
        return session.getSchema().orElse(null);
    }

    @Override
    public String getPath()
    {
        return session.getPath().toString();
    }

    @Override
    public String getSource()
    {
        return session.getSource().orElse(null);
    }

    @Override
    public Optional<String> getTraceToken()
    {
        return session.getTraceToken();
    }

    @Override
    public String getRemoteUserAddress()
    {
        return session.getRemoteUserAddress().orElse(null);
    }

    @Override
    public String getUserAgent()
    {
        return session.getUserAgent().orElse(null);
    }

    @Override
    public String getClientInfo()
    {
        return session.getClientInfo().orElse(null);
    }

    @Override
    public Set<String> getClientTags()
    {
        return session.getClientTags();
    }

    @Override
    public Set<String> getClientCapabilities()
    {
        return session.getClientCapabilities();
    }

    @Override
    public ResourceEstimates getResourceEstimates()
    {
        return session.getResourceEstimates();
    }

    @Override
    public String getTimeZoneId()
    {
        return session.getTimeZoneKey().getId();
    }

    @Override
    public String getLanguage()
    {
        return session.getLocale().getLanguage();
    }

    @Override
    public Map<String, String> getSystemProperties()
    {
        return session.getSystemProperties();
    }

    @Override
    public Map<String, Map<String, String>> getCatalogSessionProperties()
    {
        ImmutableMap.Builder<String, Map<String, String>> catalogSessionProperties = ImmutableMap.builder();
        for (Entry<ConnectorId, Map<String, String>> entry : session.getConnectorProperties().entrySet()) {
            catalogSessionProperties.put(entry.getKey().getCatalogName(), entry.getValue());
        }
        return catalogSessionProperties.build();
    }

    @Override
    public Map<String, String> getPreparedStatements()
    {
        return session.getPreparedStatements();
    }

    @Override
    public Optional<TransactionId> getTransactionId()
    {
        return session.getTransactionId();
    }

    @Override
    public boolean supportClientTransaction()
    {
        return session.isClientTransactionSupport();
    }
}
