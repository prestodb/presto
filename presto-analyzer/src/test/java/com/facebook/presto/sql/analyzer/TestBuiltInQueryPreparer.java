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

import com.facebook.presto.common.resourceGroups.QueryType;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.analyzer.AnalyzerOptions;
import com.facebook.presto.spi.procedure.DistributedProcedure;
import com.facebook.presto.spi.procedure.IProcedureRegistry;
import com.facebook.presto.spi.procedure.Procedure;
import com.facebook.presto.spi.procedure.TestProcedureRegistry;
import com.facebook.presto.sql.analyzer.BuiltInQueryPreparer.BuiltInPreparedQuery;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.Call;
import com.facebook.presto.sql.tree.CallArgument;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.StringLiteral;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.type.StandardTypes.VARCHAR;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.spi.procedure.DistributedProcedure.SCHEMA;
import static com.facebook.presto.spi.procedure.DistributedProcedure.TABLE_NAME;
import static com.facebook.presto.sql.QueryUtil.selectList;
import static com.facebook.presto.sql.QueryUtil.simpleQuery;
import static com.facebook.presto.sql.QueryUtil.table;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_PARAMETER_USAGE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestBuiltInQueryPreparer
{
    private static final SqlParser SQL_PARSER = new SqlParser();
    private static final Map<String, String> emptyPreparedStatements = ImmutableMap.of();
    private static final AnalyzerOptions testAnalyzerOptions = AnalyzerOptions.builder().build();
    private static IProcedureRegistry procedureRegistry;
    private static BuiltInQueryPreparer queryPreparer;

    @BeforeClass
    public void setup()
    {
        procedureRegistry = new TestProcedureRegistry();
        List<Procedure.Argument> arguments = new ArrayList<>();
        arguments.add(new Procedure.Argument(SCHEMA, VARCHAR));
        arguments.add(new Procedure.Argument(TABLE_NAME, VARCHAR));

        List<Procedure> procedures = new ArrayList<>();
        procedures.add(new Procedure("system", "fun", arguments));
        procedures.add(new DistributedProcedure("system", "distributed_fun",
                arguments,
                (session, transactionContext, procedureHandle, fragments) -> null,
                (transactionContext, procedureHandle, fragments) -> {}));
        procedureRegistry.addProcedures(new ConnectorId("test"), procedures);
        queryPreparer = new BuiltInQueryPreparer(SQL_PARSER, procedureRegistry);
    }

    @Test
    public void testSelectStatement()
    {
        BuiltInPreparedQuery preparedQuery = queryPreparer.prepareQuery(testAnalyzerOptions, "SELECT * FROM foo", emptyPreparedStatements, WarningCollector.NOOP);
        assertEquals(preparedQuery.getStatement(),
                simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("foo"))));
    }

    @Test
    public void testCallProcedureStatement()
    {
        BuiltInPreparedQuery preparedQuery = queryPreparer.prepareQuery(testAnalyzerOptions, "call test.system.fun('a', 'b')", emptyPreparedStatements, WarningCollector.NOOP);
        List<CallArgument> arguments = new ArrayList<>();
        arguments.add(new CallArgument(new StringLiteral("a")));
        arguments.add(new CallArgument(new StringLiteral("b")));
        assertEquals(preparedQuery.getStatement(),
                new Call(QualifiedName.of("test", "system", "fun"), arguments));
        assertTrue(preparedQuery.getQueryType().isPresent());
        assertEquals(preparedQuery.getQueryType().get(), QueryType.DATA_DEFINITION);
    }

    @Test
    public void testCallDistributedProcedureStatement()
    {
        BuiltInPreparedQuery preparedQuery = queryPreparer.prepareQuery(testAnalyzerOptions, "call test.system.distributed_fun('a', 'b')", emptyPreparedStatements, WarningCollector.NOOP);
        List<CallArgument> arguments = new ArrayList<>();
        arguments.add(new CallArgument(new StringLiteral("a")));
        arguments.add(new CallArgument(new StringLiteral("b")));
        assertEquals(preparedQuery.getStatement(),
                new Call(QualifiedName.of("test", "system", "distributed_fun"), arguments));
        assertTrue(preparedQuery.getQueryType().isPresent());
        assertEquals(preparedQuery.getQueryType().get(), QueryType.CALL_DISTRIBUTED_PROCEDURE);
    }

    @Test
    public void testExecuteStatement()
    {
        Map<String, String> preparedStatements = ImmutableMap.of("my_query", "SELECT * FROM foo");
        BuiltInPreparedQuery preparedQuery = queryPreparer.prepareQuery(testAnalyzerOptions, "EXECUTE my_query", preparedStatements, WarningCollector.NOOP);
        assertEquals(preparedQuery.getStatement(),
                simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("foo"))));
    }

    @Test
    public void testExecuteStatementDoesNotExist()
    {
        try {
            queryPreparer.prepareQuery(testAnalyzerOptions, "execute my_query", emptyPreparedStatements, WarningCollector.NOOP);
            fail("expected exception");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), NOT_FOUND.toErrorCode());
        }
    }

    @Test
    public void testTooManyParameters()
    {
        try {
            Map<String, String> preparedStatements = ImmutableMap.of("my_query", "SELECT * FROM foo where col1 = ?");
            queryPreparer.prepareQuery(testAnalyzerOptions, "EXECUTE my_query USING 1,2", preparedStatements, WarningCollector.NOOP);
            fail("expected exception");
        }
        catch (SemanticException e) {
            assertEquals(e.getCode(), INVALID_PARAMETER_USAGE);
        }
    }

    @Test
    public void testTooFewParameters()
    {
        try {
            Map<String, String> preparedStatements = ImmutableMap.of("my_query", "SELECT ? FROM foo where col1 = ?");
            queryPreparer.prepareQuery(testAnalyzerOptions, "EXECUTE my_query USING 1", preparedStatements, WarningCollector.NOOP);
            fail("expected exception");
        }
        catch (SemanticException e) {
            assertEquals(e.getCode(), INVALID_PARAMETER_USAGE);
        }
    }

    @Test
    public void testFormattedQuery()
    {
        AnalyzerOptions analyzerOptions = AnalyzerOptions.builder().setLogFormattedQueryEnabled(true).build();
        BuiltInPreparedQuery preparedQuery = queryPreparer.prepareQuery(
                analyzerOptions,
                "PREPARE test FROM SELECT * FROM foo where col1 = ?",
                emptyPreparedStatements,
                WarningCollector.NOOP);
        assertEquals(preparedQuery.getFormattedQuery(), Optional.of("-- Formatted Query:\n" +
                "PREPARE test FROM\n" +
                "   SELECT *\n" +
                "   FROM\n" +
                "     foo\n" +
                "   WHERE (col1 = ?)\n"));

        preparedQuery = queryPreparer.prepareQuery(
                analyzerOptions,
                "PREPARE test FROM SELECT * FROM foo",
                emptyPreparedStatements,
                WarningCollector.NOOP);
        assertEquals(preparedQuery.getFormattedQuery(), Optional.of("-- Formatted Query:\n" +
                "PREPARE test FROM\n" +
                "   SELECT *\n" +
                "   FROM\n" +
                "     foo\n"));
    }
}
