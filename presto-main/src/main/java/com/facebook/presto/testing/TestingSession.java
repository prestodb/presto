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
package com.facebook.presto.testing;

import com.facebook.presto.Session;
import com.facebook.presto.Session.SessionBuilder;
import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.connector.system.StaticSystemTablesProvider;
import com.facebook.presto.connector.system.SystemTablesMetadata;
import com.facebook.presto.execution.QueryIdGenerator;
import com.facebook.presto.metadata.Catalog;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.facebook.presto.sql.SqlPath;
import com.google.common.collect.ImmutableSet;

import java.util.Optional;

import static com.facebook.presto.connector.ConnectorId.createInformationSchemaConnectorId;
import static com.facebook.presto.connector.ConnectorId.createSystemTablesConnectorId;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static java.util.Locale.ENGLISH;

public final class TestingSession
{
    public static final String TESTING_CATALOG = "testing_catalog";
    private static final QueryIdGenerator queryIdGenerator = new QueryIdGenerator();

    private TestingSession() {}

    public static SessionBuilder testSessionBuilder()
    {
        return testSessionBuilder(new SessionPropertyManager());
    }

    public static SessionBuilder testSessionBuilder(SessionPropertyManager sessionPropertyManager)
    {
        return Session.builder(sessionPropertyManager)
                .setQueryId(queryIdGenerator.createNextQueryId())
                .setIdentity(new Identity("user", Optional.empty()))
                .setSource("test")
                .setCatalog("catalog")
                .setSchema("schema")
                .setPath(new SqlPath(Optional.of("path")))
                .setTimeZoneKey(UTC_KEY)
                .setLocale(ENGLISH)
                .setRemoteUserAddress("address")
                .setUserAgent("agent");
    }

    public static Catalog createBogusTestingCatalog(String catalogName)
    {
        ConnectorId connectorId = new ConnectorId(catalogName);
        return new Catalog(
                catalogName,
                connectorId,
                createTestSessionConnector(),
                createInformationSchemaConnectorId(connectorId),
                createTestSessionConnector(),
                createSystemTablesConnectorId(connectorId),
                createTestSessionConnector());
    }

    private static Connector createTestSessionConnector()
    {
        return new Connector()
        {
            @Override
            public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
            {
                return new ConnectorTransactionHandle() {};
            }

            @Override
            public ConnectorMetadata getMetadata(ConnectorTransactionHandle transaction)
            {
                return new SystemTablesMetadata(new ConnectorId("test_session_connector"), new StaticSystemTablesProvider(ImmutableSet.of()));
            }

            @Override
            public ConnectorSplitManager getSplitManager()
            {
                throw new UnsupportedOperationException();
            }
        };
    }
}
