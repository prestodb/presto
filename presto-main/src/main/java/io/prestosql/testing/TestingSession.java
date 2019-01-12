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
package io.prestosql.testing;

import com.google.common.collect.ImmutableSet;
import io.prestosql.Session;
import io.prestosql.Session.SessionBuilder;
import io.prestosql.connector.ConnectorId;
import io.prestosql.connector.system.StaticSystemTablesProvider;
import io.prestosql.connector.system.SystemTablesMetadata;
import io.prestosql.execution.QueryIdGenerator;
import io.prestosql.metadata.Catalog;
import io.prestosql.metadata.SessionPropertyManager;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.security.Identity;
import io.prestosql.spi.transaction.IsolationLevel;
import io.prestosql.spi.type.TimeZoneKey;
import io.prestosql.sql.SqlPath;

import java.util.Optional;

import static io.prestosql.connector.ConnectorId.createInformationSchemaConnectorId;
import static io.prestosql.connector.ConnectorId.createSystemTablesConnectorId;
import static java.util.Locale.ENGLISH;

public final class TestingSession
{
    public static final String TESTING_CATALOG = "testing_catalog";
    private static final QueryIdGenerator queryIdGenerator = new QueryIdGenerator();

    /*
     * Pacific/Apia
     *  - has DST (e.g. January 2017)
     *  - had DST change at midnight (on Sunday, 26 September 2010, 00:00:00 clocks were turned forward 1 hour)
     *  - had offset change since 1970 (offset in January 1970: -11:00, offset in January 2017: +14:00, offset in June 2017: +13:00)
     *  - a whole day was skipped during policy change (on Friday, 30 December 2011, 00:00:00 clocks were turned forward 24 hours)
     */
    public static final TimeZoneKey DEFAULT_TIME_ZONE_KEY = TimeZoneKey.getTimeZoneKey("Pacific/Apia");

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
                .setTimeZoneKey(DEFAULT_TIME_ZONE_KEY)
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
