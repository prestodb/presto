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
package com.facebook.presto.sql.analyzer;

import com.facebook.presto.Session;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.MaterializedViewDefinition;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.analyzer.ViewDefinitionReferences;
import com.facebook.presto.spi.security.AccessControl;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.security.ViewAccessControl;
import com.facebook.presto.spi.security.ViewSecurity;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.CreateMaterializedView;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.Parameter;
import com.facebook.presto.sql.tree.Property;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.LEGACY_TIMESTAMP_WITH_TIMEZONE;
import static com.facebook.presto.SystemSessionProperties.MATERIALIZED_VIEW_ON_MATERIALIZED_VIEW_ENABLED;
import static com.facebook.presto.SystemSessionProperties.isLegacyTimestampWithTimezone;
import static com.facebook.presto.spi.security.ViewSecurity.DEFINER;
import static com.facebook.presto.spi.security.ViewSecurity.INVOKER;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.sql.analyzer.utils.ParameterUtils.parameterExtractor;
import static com.facebook.presto.util.AnalyzerUtil.createParsingOptions;
import static java.util.Objects.requireNonNull;

public final class MaterializedViewColumnDerivationResolver
{
    private final Session session;
    private final Metadata metadata;
    private final SqlParser sqlParser;
    private final AccessControl accessControl;
    private final WarningCollector warningCollector;
    private final Map<QualifiedObjectName, Map<String, MaterializedViewColumnDerivation>> resolved = new HashMap<>();
    private final Set<QualifiedObjectName> resolving = new HashSet<>();

    public MaterializedViewColumnDerivationResolver(
            Session session,
            Metadata metadata,
            SqlParser sqlParser,
            AccessControl accessControl,
            WarningCollector warningCollector)
    {
        this.session = requireNonNull(session, "session is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
    }

    Map<String, MaterializedViewColumnDerivation> resolve(
            QualifiedObjectName viewName,
            MaterializedViewDefinition definition)
    {
        Map<String, MaterializedViewColumnDerivation> cached = resolved.get(viewName);
        if (cached != null) {
            return cached;
        }
        if (!resolving.add(viewName)) {
            throw new SemanticException(NOT_SUPPORTED, "Materialized view derivation is recursive: %s", viewName);
        }

        try {
            ViewSecurity securityMode = definition.getSecurityMode().orElse(INVOKER);
            Identity identity = getAnalysisIdentity(definition, securityMode);
            AccessControl analysisAccessControl = securityMode == DEFINER && !identity.getUser().equals(session.getIdentity().getUser())
                    ? new ViewAccessControl(accessControl)
                    : accessControl;
            Session analysisSession = createAnalysisSession(viewName, identity);
            Query query = (Query) sqlParser.createStatement(
                    definition.getOriginalSql(),
                    createParsingOptions(analysisSession, warningCollector));
            CreateMaterializedView statement = new CreateMaterializedView(
                    QualifiedName.of(viewName.getCatalogName(), viewName.getSchemaName(), viewName.getObjectName()),
                    query,
                    false,
                    definition.getSecurityMode(),
                    ImmutableList.<Property>of(),
                    Optional.empty());
            Map<NodeRef<Parameter>, Expression> parameters = parameterExtractor(statement, ImmutableList.of());
            Analyzer analyzer = new Analyzer(
                    analysisSession,
                    metadata,
                    sqlParser,
                    analysisAccessControl,
                    Optional.empty(),
                    ImmutableList.of(),
                    parameters,
                    warningCollector,
                    definition.getOriginalSql(),
                    new ViewDefinitionReferences());
            Analysis analysis = analyzer.analyzeMaterializedViewDefinition(statement);
            Map<String, MaterializedViewColumnDerivation> derivations = ImmutableMap.copyOf(
                    new MaterializedViewColumnMappingExtractor(analysis, analysisSession, metadata, this)
                            .getMaterializedViewColumnDerivations());
            resolved.put(viewName, derivations);
            return derivations;
        }
        catch (SemanticException e) {
            throw new SemanticException(
                    NOT_SUPPORTED,
                    "Failed analyzing materialized view %s while deriving its output columns: %s",
                    viewName,
                    e.getMessage());
        }
        catch (RuntimeException e) {
            throw new SemanticException(
                    NOT_SUPPORTED,
                    e,
                    Optional.empty(),
                    "Failed analyzing materialized view %s while deriving its output columns: %s",
                    viewName,
                    e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage());
        }
        finally {
            resolving.remove(viewName);
        }
    }

    private Identity getAnalysisIdentity(MaterializedViewDefinition definition, ViewSecurity securityMode)
    {
        if (securityMode != DEFINER) {
            return session.getIdentity();
        }
        String owner = definition.getOwner()
                .orElseThrow(() -> new SemanticException(NOT_SUPPORTED, "Owner must be present for DEFINER security mode"));
        if (owner.equals(session.getIdentity().getUser())) {
            return session.getIdentity();
        }
        return new Identity(
                owner,
                Optional.empty(),
                ImmutableMap.of(),
                session.getIdentity().getExtraCredentials(),
                ImmutableMap.of(),
                Optional.empty(),
                session.getIdentity().getReasonForSelect(),
                ImmutableList.of());
    }

    private Session createAnalysisSession(QualifiedObjectName viewName, Identity identity)
    {
        Session.SessionBuilder builder = Session.builder(metadata.getSessionPropertyManager())
                .setQueryId(session.getQueryId())
                .setRuntimeStats(session.getRuntimeStats())
                .setTransactionId(session.getTransactionId().orElse(null))
                .setIdentity(identity)
                .setSource(session.getSource().orElse(null))
                .setCatalog(viewName.getCatalogName())
                .setSchema(viewName.getSchemaName())
                .setTimeZoneKey(session.getTimeZoneKey())
                .setLocale(session.getLocale())
                .setSystemProperty(LEGACY_TIMESTAMP_WITH_TIMEZONE, Boolean.toString(isLegacyTimestampWithTimezone(session)))
                .setRemoteUserAddress(session.getRemoteUserAddress().orElse(null))
                .setUserAgent(session.getUserAgent().orElse(null))
                .setClientInfo(session.getClientInfo().orElse(null))
                .setStartTime(session.getStartTime());
        if (identity.getUser().equals(session.getIdentity().getUser())) {
            session.getSystemProperties().forEach(builder::setSystemProperty);
        }
        else {
            String enabled = session.getSystemProperties().get(MATERIALIZED_VIEW_ON_MATERIALIZED_VIEW_ENABLED);
            if (enabled != null) {
                builder.setSystemProperty(MATERIALIZED_VIEW_ON_MATERIALIZED_VIEW_ENABLED, enabled);
            }
        }
        session.getConnectorProperties().forEach((connectorId, properties) -> properties.forEach((key, value) -> builder.setConnectionProperty(connectorId, key, value)));
        return builder.build();
    }
}
