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

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.metadata.ViewDefinition;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.Analyzer;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.analyzer.Field;
import com.facebook.presto.sql.analyzer.QueryExplainer;
import com.facebook.presto.sql.parser.ParsingException;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.tree.CreateView;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Statement;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;

import javax.inject.Inject;

import java.util.List;

import static com.facebook.presto.metadata.MetadataUtil.createQualifiedTableName;
import static com.facebook.presto.metadata.ViewDefinition.ViewColumn;
import static com.facebook.presto.spi.StandardErrorCode.INTERNAL_ERROR;
import static com.facebook.presto.sql.SqlFormatter.formatSql;
import static com.google.common.base.Preconditions.checkNotNull;

public class CreateViewTask
        implements DataDefinitionTask<CreateView>
{
    private final JsonCodec<ViewDefinition> codec;
    private final SqlParser sqlParser;
    private final List<PlanOptimizer> planOptimizers;
    private final boolean experimentalSyntaxEnabled;

    @Inject
    public CreateViewTask(JsonCodec<ViewDefinition> codec, SqlParser sqlParser, List<PlanOptimizer> planOptimizers, FeaturesConfig featuresConfig)
    {
        this.codec = checkNotNull(codec, "codec is null");
        this.sqlParser = checkNotNull(sqlParser, "sqlParser is null");
        this.planOptimizers = ImmutableList.copyOf(checkNotNull(planOptimizers, "planOptimizers is null"));
        this.experimentalSyntaxEnabled = checkNotNull(featuresConfig, "featuresConfig is null").isExperimentalSyntaxEnabled();
    }

    @Override
    public void execute(CreateView statement, ConnectorSession session, Metadata metadata)
    {
        QualifiedTableName name = createQualifiedTableName(session, statement.getName());

        String sql = getFormattedSql(statement);

        Analysis analysis = analyzeStatement(statement, session, metadata);
        Iterable<Field> fields = analysis.getOutputDescriptor().getVisibleFields();

        List<ViewColumn> columns = FluentIterable.from(fields).transform(fieldToColumn()).toList();

        String data = codec.toJson(new ViewDefinition(sql, session.getCatalog(), session.getSchema(), columns));

        metadata.createView(session, name, data, statement.isReplace());
    }

    public Analysis analyzeStatement(Statement statement, ConnectorSession session, Metadata metadata)
    {
        QueryExplainer explainer = new QueryExplainer(session, planOptimizers, metadata, sqlParser, experimentalSyntaxEnabled);
        Analyzer analyzer = new Analyzer(session, metadata, sqlParser, Optional.of(explainer), experimentalSyntaxEnabled);
        return analyzer.analyze(statement);
    }

    private static Function<Field, ViewColumn> fieldToColumn()
    {
        return new Function<Field, ViewColumn>()
        {
            @Override
            public ViewColumn apply(Field field)
            {
                return new ViewColumn(field.getName().get(), field.getType());
            }
        };
    }

    private String getFormattedSql(CreateView statement)
    {
        Query query = statement.getQuery();
        String sql = formatSql(query);

        // verify round-trip
        Statement parsed;
        try {
            parsed = sqlParser.createStatement(sql);
        }
        catch (ParsingException e) {
            throw new PrestoException(INTERNAL_ERROR.toErrorCode(), "Formatted query does not parse: " + query);
        }
        if (!query.equals(parsed)) {
            throw new PrestoException(INTERNAL_ERROR.toErrorCode(), "Query does not round-trip: " + query);
        }

        return sql;
    }
}
