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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.analyzer.MetadataResolver;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.Table;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.common.RuntimeMetricName.GET_TABLE_HANDLE_TIME_NANOS;
import static com.facebook.presto.metadata.MetadataUtil.createQualifiedObjectName;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_SCHEMA;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_TABLE;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;

/**
 * TODO: Move executorService to more global level
 * TODO: create a feature flag
 * TODO: Test in the cluster for performance.
 */
public class MetadataExtractor
{
    private static final Logger log = Logger.get(MetadataExtractor.class);

    private final Session session;
    private final MetadataResolver metadataResolver;
    static ExecutorService executorService = newCachedThreadPool(daemonThreadsNamed("metadata-extractor-%s"));

    public MetadataExtractor(Session session, MetadataResolver metadataResolver)
    {
        this.session = requireNonNull(session, "session is null");
        this.metadataResolver = requireNonNull(metadataResolver, "metadataResolver is null");
    }

    public void populateMetadataHandle(Statement statement, MetadataHandle metadataHandle)
    {
        MetadataExtractorContext metadataExtractorContext = new MetadataExtractorContext();
        Visitor visitor = new Visitor(session);
        visitor.process(statement, metadataExtractorContext);

        log.info("populateMetadataHandle total table count is %s", metadataExtractorContext.getTableNames().size());
        metadataExtractorContext.getTableNames().forEach(tableName -> {
            log.info("populateMetadataHandle processing tableName: %s", tableName);
            if (tableName.getObjectName().isEmpty()) {
                throw new SemanticException(MISSING_TABLE, "Table name is empty");
            }
            if (tableName.getSchemaName().isEmpty()) {
                throw new SemanticException(MISSING_SCHEMA, "Schema name is empty");
            }

/*
            metadataHandle.addViewDefinition(tableName, executorService.submit(() -> {
                Optional<ViewDefinition> optionalView = session.getRuntimeStats().profileNanos(
                        GET_VIEW_TIME_NANOS,
                        () -> metadataResolver.getView(tableName));
                return optionalView;
            }));

            metadataHandle.addMaterializedViewDefinition(tableName, executorService.submit(() -> {
                Optional<MaterializedViewDefinition> optionalMaterializedView = session.getRuntimeStats().profileNanos(
                        GET_MATERIALIZED_VIEW_TIME_NANOS,
                        () -> metadataResolver.getMaterializedView(tableName));
                return optionalMaterializedView;
            }));
*/

            metadataHandle.addTableHandle(tableName, executorService.submit(() -> {
                Optional<TableHandle> tableHandle = session.getRuntimeStats().profileNanos(
                        GET_TABLE_HANDLE_TIME_NANOS,
                        () -> metadataResolver.getTableHandle(tableName));
                return tableHandle;
            }));
        });
    }

    private class MetadataExtractorContext
    {
        private final Set<QualifiedObjectName> tableNames = new HashSet<>();

        public void addTable(QualifiedObjectName tableName)
        {
            tableNames.add(tableName);
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
    }
}
