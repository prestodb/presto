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
package com.facebook.presto.server.protocol;

import com.facebook.presto.client.CreateQuerySession;
import com.facebook.presto.server.ParsePropertiesUtils.SystemAndCatalogProperties;
import com.facebook.presto.server.SessionContext;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.transaction.TransactionId;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import javax.servlet.http.HttpServletRequest;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.server.ParsePropertiesUtils.assertRequest;
import static com.facebook.presto.server.ParsePropertiesUtils.parseSessionProperties;
import static com.google.common.base.Strings.isNullOrEmpty;

public final class QueryRequestSessionContext
        implements SessionContext
{
    private final CreateQuerySession createQuerySession;
    private final Identity identity;
    private final String remoteUserAddress;
    private final Map<String, String> systemProperties;
    private final Map<String, Map<String, String>> catalogSessionProperties;
    private final Optional<TransactionId> transactionId;

    public QueryRequestSessionContext(CreateQuerySession createQuerySession, HttpServletRequest servletRequest)
    {
        assertRequest(createQuerySession != null, "createQuerySession is null");
        this.createQuerySession = createQuerySession;
        assertRequest(!isNullOrEmpty(createQuerySession.getUser()), "User must be set");
        this.identity = new Identity(createQuerySession.getUser(), Optional.ofNullable(servletRequest.getUserPrincipal()));
        this.remoteUserAddress = servletRequest.getRemoteAddr();
        if (createQuerySession.getProperties() != null && !createQuerySession.getProperties().isEmpty()) {
            SystemAndCatalogProperties systemAndCatalogProperties = parseSessionProperties(createQuerySession.getProperties(), "Invalid properties object");
            this.systemProperties = systemAndCatalogProperties.getSystemProperties();
            this.catalogSessionProperties = systemAndCatalogProperties.getCatalogProperties();
        }
        else {
            this.systemProperties = ImmutableMap.of();
            this.catalogSessionProperties = ImmutableMap.of();
        }
        this.transactionId = Optional.ofNullable(createQuerySession.getTransactionId()).map(TransactionId::valueOf);
    }

    @Override
    public Identity getIdentity()
    {
        return identity;
    }

    @Override
    public String getCatalog()
    {
        return createQuerySession.getCatalog();
    }

    @Override
    public String getSchema()
    {
        return createQuerySession.getSchema();
    }

    @Override
    public String getSource()
    {
        return createQuerySession.getSource();
    }

    @Override
    public String getRemoteUserAddress()
    {
        return remoteUserAddress;
    }

    @Override
    public String getUserAgent()
    {
        return createQuerySession.getUserAgent();
    }

    @Override
    public String getClientInfo()
    {
        return createQuerySession.getClientInfo();
    }

    @Override
    public Set<String> getClientTags()
    {
        return createQuerySession.getClientTags() == null ? ImmutableSet.of() : createQuerySession.getClientTags();
    }

    @Override
    public String getTimeZoneId()
    {
        return createQuerySession.getTimeZoneId();
    }

    @Override
    public String getLanguage()
    {
        return createQuerySession.getLanguage();
    }

    @Override
    public Map<String, String> getSystemProperties()
    {
        return systemProperties;
    }

    @Override
    public Map<String, Map<String, String>> getCatalogSessionProperties()
    {
        return catalogSessionProperties;
    }

    @Override
    public Map<String, String> getPreparedStatements()
    {
        return createQuerySession.getPreparedStatements() == null ? ImmutableMap.of() : createQuerySession.getPreparedStatements();
    }

    @Override
    public Optional<TransactionId> getTransactionId()
    {
        return transactionId;
    }

    @Override
    public boolean supportClientTransaction()
    {
        return createQuerySession.supportClientTransaction();
    }
}
