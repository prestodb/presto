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
package com.facebook.presto.tests;

import com.facebook.presto.index.IndexManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.analyzer.QueryExplainer;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.PlanOptimizersFactory;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.tree.ExplainType;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.base.Function;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;

import java.util.List;

import static org.testng.Assert.assertEquals;

public abstract class AbstractTestQueryFramework
{
    protected final H2QueryRunner h2QueryRunner;
    protected final QueryRunner queryRunner;
    private final SqlParser sqlParser;

    protected AbstractTestQueryFramework(QueryRunner queryRunner)
    {
        this.queryRunner = queryRunner;
        h2QueryRunner = new H2QueryRunner();
        sqlParser = new SqlParser();
    }

    @AfterClass(alwaysRun = true)
    private void close()
            throws Exception
    {
        try {
            h2QueryRunner.close();
        }
        finally {
            queryRunner.close();
        }
    }

    protected ConnectorSession getSession()
    {
        return queryRunner.getDefaultSession();
    }

    public final int getNodeCount()
    {
        return queryRunner.getNodeCount();
    }

    protected MaterializedResult computeActual(@Language("SQL") String sql)
    {
        return queryRunner.execute(getSession(), sql).toJdbcTypes();
    }

    protected void assertQuery(@Language("SQL") String sql)
            throws Exception
    {
        QueryAssertions.assertQuery(queryRunner, getSession(), sql, h2QueryRunner, sql, false);
    }

    public void assertQueryOrdered(@Language("SQL") String sql)
            throws Exception
    {
        QueryAssertions.assertQuery(queryRunner, getSession(), sql, h2QueryRunner, sql, true);
    }

    protected void assertQuery(@Language("SQL") String actual, @Language("SQL") String expected)
            throws Exception
    {
        QueryAssertions.assertQuery(queryRunner, getSession(), actual, h2QueryRunner, expected, false);
    }

    protected void assertQueryOrdered(@Language("SQL") String actual, @Language("SQL") String expected)
            throws Exception
    {
        QueryAssertions.assertQuery(queryRunner, getSession(), actual, h2QueryRunner, expected, true);
    }

    protected void assertQueryTrue(@Language("SQL") String sql)
            throws Exception
    {
        assertQuery(sql, "SELECT true");
    }

    public void assertApproximateQuery(ConnectorSession session, @Language("SQL") String actual, @Language("SQL") String expected)
            throws Exception
    {
        QueryAssertions.assertApproximateQuery(queryRunner,
                session,
                actual,
                h2QueryRunner,
                expected);
    }

    protected MaterializedResult computeExpected(@Language("SQL") String sql, List<? extends Type> resultTypes)
    {
        return h2QueryRunner.execute(sql, resultTypes);
    }

    public Function<MaterializedRow, String> onlyColumnGetter()
    {
        return new Function<MaterializedRow, String>()
        {
            @Override
            public String apply(MaterializedRow input)
            {
                assertEquals(input.getFieldCount(), 1);
                return (String) input.getField(0);
            }
        };
    }

    public String getExplainPlan(String query, ExplainType.Type planType)
    {
        QueryExplainer explainer = getQueryExplainer();
        return explainer.getPlan(sqlParser.createStatement(query), planType);
    }

    public String getGraphvizExplainPlan(String query, ExplainType.Type planType)
    {
        QueryExplainer explainer = getQueryExplainer();
        return explainer.getGraphvizPlan(sqlParser.createStatement(query), planType);
    }

    private QueryExplainer getQueryExplainer()
    {
        Metadata metadata = new MetadataManager(new FeaturesConfig().setExperimentalSyntaxEnabled(true), new TypeRegistry());
        FeaturesConfig featuresConfig = new FeaturesConfig().setExperimentalSyntaxEnabled(true);
        List<PlanOptimizer> optimizers = new PlanOptimizersFactory(metadata, sqlParser, new SplitManager(), new IndexManager(), featuresConfig).get();
        return new QueryExplainer(queryRunner.getDefaultSession(), optimizers, metadata, sqlParser, featuresConfig.isExperimentalSyntaxEnabled(), featuresConfig.isDistributedIndexJoinsEnabled());
    }
}
