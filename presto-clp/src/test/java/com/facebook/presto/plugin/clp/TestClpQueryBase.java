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
package com.facebook.presto.plugin.clp;

import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.common.block.BlockEncodingManager;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.AnalyzePropertyManager;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.metadata.ColumnPropertyManager;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.SchemaPropertyManager;
import com.facebook.presto.metadata.TablePropertyManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.SqlToRowExpressionTranslator;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.testing.TestingSession;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.facebook.presto.metadata.SessionPropertyManager.createTestingSessionPropertyManager;
import static com.facebook.presto.spi.WarningCollector.NOOP;
import static com.facebook.presto.sql.ExpressionUtils.rewriteIdentifiersToSymbolReferences;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypes;
import static com.facebook.presto.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static com.facebook.presto.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static java.util.stream.Collectors.toMap;

public class TestClpQueryBase
{
    protected static final FunctionAndTypeManager functionAndTypeManager = createTestFunctionAndTypeManager();
    protected static final StandardFunctionResolution standardFunctionResolution = new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver());
    protected static final Metadata metadata = new MetadataManager(
            functionAndTypeManager,
            new BlockEncodingManager(),
            createTestingSessionPropertyManager(),
            new SchemaPropertyManager(),
            new TablePropertyManager(),
            new ColumnPropertyManager(),
            new AnalyzePropertyManager(),
            createTestTransactionManager(new CatalogManager()));

    protected static final ClpTableHandle table = new ClpTableHandle(new SchemaTableName("default", "test"), "");
    protected static final ClpColumnHandle city = new ClpColumnHandle(
            "city",
            RowType.from(ImmutableList.of(
                    RowType.field("Region", RowType.from(ImmutableList.of(
                            RowType.field("Id", BIGINT),
                            RowType.field("Name", VARCHAR)))),
                    RowType.field("Name", VARCHAR))),
            true);
    protected static final ClpColumnHandle fare = new ClpColumnHandle("fare", DOUBLE, true);
    protected static final ClpColumnHandle isHoliday = new ClpColumnHandle("isHoliday", BOOLEAN, true);
    protected static final Map<VariableReferenceExpression, ColumnHandle> variableToColumnHandleMap =
            Stream.of(city, fare, isHoliday)
                    .collect(toMap(
                            ch -> new VariableReferenceExpression(Optional.empty(), ch.getColumnName(), ch.getColumnType()),
                            ch -> ch));
    protected final TypeProvider typeProvider = TypeProvider.fromVariables(variableToColumnHandleMap.keySet());

    public static Expression expression(String sql)
    {
        return rewriteIdentifiersToSymbolReferences(
                new SqlParser().createExpression(sql, ParsingOptions.builder().setDecimalLiteralTreatment(AS_DECIMAL).build()));
    }

    protected RowExpression toRowExpression(Expression expression, TypeProvider typeProvider, Session session)
    {
        Map<NodeRef<Expression>, Type> expressionTypes = getExpressionTypes(
                session,
                metadata,
                new SqlParser(),
                typeProvider,
                expression,
                ImmutableMap.of(),
                NOOP);
        return SqlToRowExpressionTranslator.translate(expression, expressionTypes, ImmutableMap.of(), functionAndTypeManager, session);
    }

    protected RowExpression getRowExpression(String sqlExpression, SessionHolder sessionHolder)
    {
        return toRowExpression(expression(sqlExpression), typeProvider, sessionHolder.getSession());
    }

    protected RowExpression getRowExpression(String sqlExpression, TypeProvider typeProvider, SessionHolder sessionHolder)
    {
        return toRowExpression(expression(sqlExpression), typeProvider, sessionHolder.getSession());
    }

    protected static class SessionHolder
    {
        private final ConnectorSession connectorSession;
        private final Session session;

        public SessionHolder()
        {
            connectorSession = SESSION;
            session = TestingSession.testSessionBuilder(createTestingSessionPropertyManager(new SystemSessionProperties().getSessionProperties())).build();
        }

        public ConnectorSession getConnectorSession()
        {
            return connectorSession;
        }

        public Session getSession()
        {
            return session;
        }
    }
}
