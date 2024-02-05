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
import com.facebook.presto.spi.analyzer.MetadataResolver;
import com.facebook.presto.spi.analyzer.ViewDefinition;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Analyze;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Delete;
import com.facebook.presto.sql.tree.Insert;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.Table;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.SystemSessionProperties.isPreProcessMetadataCalls;
import static com.facebook.presto.common.RuntimeMetricName.GET_MATERIALIZED_VIEW_TIME_NANOS;
import static com.facebook.presto.common.RuntimeMetricName.GET_VIEW_TIME_NANOS;
import static com.facebook.presto.metadata.MetadataUtil.createQualifiedObjectName;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_SCHEMA;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_TABLE;
import static com.facebook.presto.util.AnalyzerUtil.createParsingOptions;
import static com.facebook.presto.util.MetadataUtils.getTableColumnMetadata;
import static java.util.Objects.requireNonNull;

public class MetadataExtractor
{
    private final Metadata metadata;
    private final MetadataResolver metadataResolver;
    private final Optional<ExecutorService> executor;
    private final SqlParser sqlParser;
    private final WarningCollector warningCollector;

    public MetadataExtractor(Session session, Metadata metadata, Optional<ExecutorService> metadataExtractorExecutor, SqlParser sqlParser, WarningCollector warningCollector)
    {
        this.metadata = requireNonNull(metadata, "metadataResolver is null");
        this.metadataResolver = requireNonNull(metadata.getMetadataResolver(session), "metadataResolver is null");
        this.executor = requireNonNull(metadataExtractorExecutor, "metadataExtractorExecutor is null");
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
    }

    public void populateMetadataHandle(Session session, Statement statement, MetadataHandle metadataHandle)
    {
        if (executor.isPresent() && isPreProcessMetadataCalls(session)) {
            metadataHandle.setPreProcessMetadataCalls(true);
            populateMetadataHandle(session, statement, metadataHandle, new MetadataExtractorContext());
        }
    }

    private void populateMetadataHandle(Session session, Statement statement, MetadataHandle metadataHandle, MetadataExtractorContext metadataExtractorContext)
    {
        Visitor visitor = new Visitor(session);
        visitor.process(statement, metadataExtractorContext);

        metadataExtractorContext.getTableNames().forEach(tableName -> {
            if (tableName.getObjectName().isEmpty()) {
                throw new SemanticException(MISSING_TABLE, "Table name is empty");
            }
            if (tableName.getSchemaName().isEmpty()) {
                throw new SemanticException(MISSING_SCHEMA, "Schema name is empty");
            }

            metadataHandle.addViewDefinition(tableName, executor.get().submit(() -> {
                Optional<ViewDefinition> optionalView = session.getRuntimeStats().profileNanos(
                        GET_VIEW_TIME_NANOS,
                        () -> metadataResolver.getView(tableName));
                if (optionalView.isPresent()) {
                    ViewDefinition view = optionalView.get();
                    Statement viewStatement = sqlParser.createStatement(view.getOriginalSql(), createParsingOptions(session, warningCollector));
                    Session.SessionBuilder viewSessionBuilder = Session.builder(metadata.getSessionPropertyManager())
                            .setQueryId(session.getQueryId())
                            .setTransactionId(session.getTransactionId().orElse(null))
                            .setIdentity(session.getIdentity())
                            .setSource(session.getSource().orElse(null))
                            .setCatalog(view.getCatalog().orElse(null))
                            .setSchema(view.getSchema().orElse(null))
                            .setTimeZoneKey(session.getTimeZoneKey())
                            .setLocale(session.getLocale())
                            .setRemoteUserAddress(session.getRemoteUserAddress().orElse(null))
                            .setUserAgent(session.getUserAgent().orElse(null))
                            .setClientInfo(session.getClientInfo().orElse(null))
                            .setStartTime(session.getStartTime());
                    populateMetadataHandle(viewSessionBuilder.build(), viewStatement, metadataHandle, new MetadataExtractorContext(Optional.of(metadataExtractorContext)));
                }
                return optionalView;
            }));

            metadataHandle.addMaterializedViewDefinition(tableName, executor.get().submit(() -> {
                Optional<MaterializedViewDefinition> optionalMaterializedView = session.getRuntimeStats().profileNanos(
                        GET_MATERIALIZED_VIEW_TIME_NANOS,
                        () -> metadataResolver.getMaterializedView(tableName));
                if (optionalMaterializedView.isPresent()) {
                    Statement materializedViewStatement = sqlParser.createStatement(optionalMaterializedView.get().getOriginalSql(), createParsingOptions(session, warningCollector));
                    populateMetadataHandle(session, materializedViewStatement, metadataHandle, new MetadataExtractorContext(Optional.of(metadataExtractorContext)));
                }
                return optionalMaterializedView;
            }));

            metadataHandle.addTableColumnMetadata(tableName, executor.get().submit(() -> getTableColumnMetadata(session, metadataResolver, tableName)));
        });
    }

    private class MetadataExtractorContext
    {
        private final Optional<MetadataExtractorContext> parent;
        private final Set<QualifiedObjectName> tableNames;

        public MetadataExtractorContext()
        {
            this.parent = Optional.empty();
            this.tableNames = new HashSet<>();
        }

        public MetadataExtractorContext(Optional<MetadataExtractorContext> parent)
        {
            this.parent = parent;
            this.tableNames = new HashSet<>();
        }

        public void addTable(QualifiedObjectName tableName)
        {
            if (!tableExists(tableName)) {
                tableNames.add(tableName);
            }
        }

        private boolean tableExists(QualifiedObjectName tableName)
        {
            if (tableNames.contains(tableName)) {
                return true;
            }

            if (parent.isPresent()) {
                return parent.get().tableExists(tableName);
            }

            return false;
        }

        public Set<QualifiedObjectName> getTableNames()
        {
            return tableNames;
        }
    }

    private class Visitor
            extends DefaultTraversalVisitor<Void, MetadataExtractorContext>
    {
        private final Session session;

        public Visitor(Session session)
        {
            this.session = requireNonNull(session, "session is null");
        }

        @Override
        protected Void visitTable(Table table, MetadataExtractorContext context)
        {
            QualifiedObjectName tableName = createQualifiedObjectName(session, table, table.getName());
            if (tableName.getObjectName().isEmpty()) {
                throw new SemanticException(MISSING_TABLE, table, "Table name is empty");
            }
            if (tableName.getSchemaName().isEmpty()) {
                throw new SemanticException(MISSING_SCHEMA, table, "Schema name is empty");
            }

            // This could be either tableName, view, or MView
            context.addTable(tableName);
            return super.visitTable(table, context);
        }

        @Override
        protected Void visitInsert(Insert insert, MetadataExtractorContext context)
        {
            QualifiedObjectName tableName = createQualifiedObjectName(session, insert, insert.getTarget());
            if (tableName.getObjectName().isEmpty()) {
                throw new SemanticException(MISSING_TABLE, insert, "Table name is empty");
            }
            if (tableName.getSchemaName().isEmpty()) {
                throw new SemanticException(MISSING_SCHEMA, insert, "Schema name is empty");
            }

            // This could be either tableName, view, or MView
            context.addTable(tableName);
            return super.visitInsert(insert, context);
        }

        @Override
        protected Void visitDelete(Delete node, MetadataExtractorContext context)
        {
            Table table = node.getTable();
            QualifiedObjectName tableName = createQualifiedObjectName(session, table, table.getName());
            if (tableName.getObjectName().isEmpty()) {
                throw new SemanticException(MISSING_TABLE, node, "Table name is empty");
            }
            if (tableName.getSchemaName().isEmpty()) {
                throw new SemanticException(MISSING_SCHEMA, node, "Schema name is empty");
            }

            context.addTable(tableName);
            return super.visitDelete(node, context);
        }

        @Override
        protected Void visitAnalyze(Analyze node, MetadataExtractorContext context)
        {
            QualifiedObjectName tableName = createQualifiedObjectName(session, node, node.getTableName());
            if (tableName.getObjectName().isEmpty()) {
                throw new SemanticException(MISSING_TABLE, node, "Table name is empty");
            }
            if (tableName.getSchemaName().isEmpty()) {
                throw new SemanticException(MISSING_SCHEMA, node, "Schema name is empty");
            }

            context.addTable(tableName);
            return super.visitAnalyze(node, context);
        }
    }
}
