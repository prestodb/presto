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
package io.prestosql.execution;

import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.json.JsonCodec;
import io.prestosql.Session;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.metadata.ViewDefinition;
import io.prestosql.security.AccessControl;
import io.prestosql.sql.analyzer.Analysis;
import io.prestosql.sql.analyzer.Analyzer;
import io.prestosql.sql.analyzer.FeaturesConfig;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.tree.CreateView;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.Statement;
import io.prestosql.transaction.TransactionManager;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.prestosql.metadata.MetadataUtil.createQualifiedObjectName;
import static io.prestosql.metadata.ViewDefinition.ViewColumn;
import static io.prestosql.sql.SqlFormatterUtil.getFormattedSql;
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

        accessControl.checkCanCreateView(session.getRequiredTransactionId(), session.getIdentity(), name);

        String sql = getFormattedSql(statement.getQuery(), sqlParser, Optional.of(parameters));

        Analysis analysis = analyzeStatement(statement, session, metadata, accessControl, parameters, stateMachine.getWarningCollector());

        List<ViewColumn> columns = analysis.getOutputDescriptor(statement.getQuery())
                .getVisibleFields().stream()
                .map(field -> new ViewColumn(field.getName().get(), field.getType()))
                .collect(toImmutableList());

        String data = codec.toJson(new ViewDefinition(sql, session.getCatalog(), session.getSchema(), columns, Optional.of(session.getUser())));

        metadata.createView(session, name, data, statement.isReplace());

        return immediateFuture(null);
    }

    private Analysis analyzeStatement(Statement statement, Session session, Metadata metadata, AccessControl accessControl, List<Expression> parameters, WarningCollector warningCollector)
    {
        Analyzer analyzer = new Analyzer(session, metadata, sqlParser, accessControl, Optional.empty(), parameters, warningCollector);
        return analyzer.analyze(statement);
    }
}
