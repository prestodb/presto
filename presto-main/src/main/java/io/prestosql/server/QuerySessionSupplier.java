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
package io.prestosql.server;

import io.prestosql.Session;
import io.prestosql.metadata.SessionPropertyManager;
import io.prestosql.security.AccessControl;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.security.Identity;
import io.prestosql.sql.SqlEnvironmentConfig;
import io.prestosql.sql.SqlPath;
import io.prestosql.transaction.TransactionManager;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static io.prestosql.Session.SessionBuilder;
import static io.prestosql.spi.type.TimeZoneKey.getTimeZoneKey;
import static java.util.Map.Entry;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class QuerySessionSupplier
        implements SessionSupplier
{
    private final TransactionManager transactionManager;
    private final AccessControl accessControl;
    private final SessionPropertyManager sessionPropertyManager;
    private final Optional<String> path;

    @Inject
    public QuerySessionSupplier(
            TransactionManager transactionManager,
            AccessControl accessControl,
            SessionPropertyManager sessionPropertyManager,
            SqlEnvironmentConfig config)
    {
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
        this.path = requireNonNull(config.getPath(), "path is null");
    }

    @Override
    public Session createSession(QueryId queryId, SessionContext context)
    {
        Identity identity = context.getIdentity();
        accessControl.checkCanSetUser(identity.getPrincipal(), identity.getUser());

        SessionBuilder sessionBuilder = Session.builder(sessionPropertyManager)
                .setQueryId(queryId)
                .setIdentity(identity)
                .setSource(context.getSource())
                .setCatalog(context.getCatalog())
                .setSchema(context.getSchema())
                .setPath(new SqlPath(path))
                .setRemoteUserAddress(context.getRemoteUserAddress())
                .setUserAgent(context.getUserAgent())
                .setClientInfo(context.getClientInfo())
                .setClientTags(context.getClientTags())
                .setClientCapabilities(context.getClientCapabilities())
                .setTraceToken(context.getTraceToken())
                .setResourceEstimates(context.getResourceEstimates());

        if (context.getPath() != null) {
            sessionBuilder.setPath(new SqlPath(Optional.of(context.getPath())));
        }

        if (context.getTimeZoneId() != null) {
            sessionBuilder.setTimeZoneKey(getTimeZoneKey(context.getTimeZoneId()));
        }

        if (context.getLanguage() != null) {
            sessionBuilder.setLocale(Locale.forLanguageTag(context.getLanguage()));
        }

        for (Entry<String, String> entry : context.getSystemProperties().entrySet()) {
            sessionBuilder.setSystemProperty(entry.getKey(), entry.getValue());
        }
        for (Entry<String, Map<String, String>> catalogProperties : context.getCatalogSessionProperties().entrySet()) {
            String catalog = catalogProperties.getKey();
            for (Entry<String, String> entry : catalogProperties.getValue().entrySet()) {
                sessionBuilder.setCatalogSessionProperty(catalog, entry.getKey(), entry.getValue());
            }
        }

        for (Entry<String, String> preparedStatement : context.getPreparedStatements().entrySet()) {
            sessionBuilder.addPreparedStatement(preparedStatement.getKey(), preparedStatement.getValue());
        }

        if (context.supportClientTransaction()) {
            sessionBuilder.setClientTransactionSupport();
        }

        Session session = sessionBuilder.build();
        if (context.getTransactionId().isPresent()) {
            session = session.beginTransactionId(context.getTransactionId().get(), transactionManager, accessControl);
        }

        return session;
    }
}
