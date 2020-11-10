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

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.Session;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.ViewDefinition;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.Analyzer;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.CreateView;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.util.concurrent.ListenableFuture;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.metadata.MetadataUtil.createQualifiedObjectName;
import static com.facebook.presto.metadata.MetadataUtil.toSchemaTableName;
import static com.facebook.presto.metadata.ViewDefinition.ViewColumn;
import static com.facebook.presto.sql.SqlFormatterUtil.getFormattedSql;
import static com.facebook.presto.sql.tree.CreateView.Security.INVOKER;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.util.Objects.requireNonNull;

public class CreateViewTask
        implements DataDefinitionTask<CreateView>
{
    private final JsonCodec<ViewDefinition> codec;
    private final SqlParser sqlParser;

    @Inject
    public CreateViewTask(
            JsonCodec<ViewDefinition> codec,
            SqlParser sqlParser,
            FeaturesConfig featuresConfig)
    {
        this.codec = requireNonNull(codec, "codec is null");
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        requireNonNull(featuresConfig, "featuresConfig is null");
    }

    @Override
    public String getName()
    {
        return "CREATE VIEW";
    }

    @Override
    public String explain(CreateView statement, List<Expression> parameters)
    {
        return "CREATE VIEW " + statement.getName();
    }

    @Override
    public ListenableFuture<?> execute(CreateView statement, TransactionManager transactionManager, Metadata metadata, AccessControl accessControl, QueryStateMachine stateMachine, List<Expression> parameters)
    {
        Session session = stateMachine.getSession();
        QualifiedObjectName name = createQualifiedObjectName(session, statement, statement.getName());

        accessControl.checkCanCreateView(session.getRequiredTransactionId(), session.getIdentity(), session.getAccessControlContext(), name);

        String sql = getFormattedSql(statement.getQuery(), sqlParser, Optional.of(parameters));

        Analysis analysis = analyzeStatement(statement, session, metadata, accessControl, parameters, stateMachine.getWarningCollector());

        List<ViewColumn> columns = analysis.getOutputDescriptor(statement.getQuery())
                .getVisibleFields().stream()
                .map(field -> new ViewColumn(field.getName().get(), field.getType()))
                .collect(toImmutableList());

        List<ColumnMetadata> columnMetadata = columns.stream()
                .map(column -> new ColumnMetadata(column.getName(), column.getType()))
                .collect(toImmutableList());

        ConnectorTableMetadata viewMetadata = new ConnectorTableMetadata(toSchemaTableName(name), columnMetadata);
        // use DEFINER security by default
        Optional<String> owner = Optional.of(session.getUser());
        if (statement.getSecurity().orElse(null) == INVOKER) {
            owner = Optional.empty();
        }

        String data = codec.toJson(new ViewDefinition(sql, session.getCatalog(), session.getSchema(), columns, owner, !owner.isPresent()));

        metadata.createView(session, name.getCatalogName(), viewMetadata, data, statement.isReplace());

        return immediateFuture(null);
    }

    private Analysis analyzeStatement(Statement statement, Session session, Metadata metadata, AccessControl accessControl, List<Expression> parameters, WarningCollector warningCollector)
    {
        Analyzer analyzer = new Analyzer(session, metadata, sqlParser, accessControl, Optional.empty(), parameters, warningCollector);
        return analyzer.analyze(statement);
    }
}
