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

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.procedure.Procedure;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Call;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.Parameter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;

public class TestCallTask
{
    private static final SqlParser SQL_PARSER = new SqlParser();
    private static final Metadata METADATA = MetadataManager.createTestMetadataManager();
    private static final Session SESSION = testSessionBuilder().build();
    private static final Map<NodeRef<Parameter>, Expression> NO_PARAMETERS = ImmutableMap.of();

    @Test
    public void testJdbcExecuteProcedureNamedArgument()
    {
        Call call = (Call) SQL_PARSER.createStatement("CALL system.execute(QUERY => 'SELECT 1')");
        Procedure procedure = new Procedure(
                "system",
                "execute",
                ImmutableList.of(new Procedure.Argument("QUERY", "varchar")));

        assertEquals(
                CallTask.extractParameterValuesInOrder(call, procedure, METADATA, SESSION, NO_PARAMETERS),
                new Object[] {"SELECT 1"});
    }

    @Test
    public void testUnquotedArgumentMatchesProcedureDeclarationCaseInsensitively()
    {
        Call lowercaseCall = (Call) SQL_PARSER.createStatement("CALL foo(lower => 1)");
        Procedure uppercaseProcedure = new Procedure(
                "schema",
                "foo",
                ImmutableList.of(new Procedure.Argument("LOWER", "bigint")));
        assertEquals(
                CallTask.extractParameterValuesInOrder(lowercaseCall, uppercaseProcedure, METADATA, SESSION, NO_PARAMETERS),
                new Object[] {1L});

        Call uppercaseCall = (Call) SQL_PARSER.createStatement("CALL foo(UPPER => 2)");
        Procedure lowercaseProcedure = new Procedure(
                "schema",
                "foo",
                ImmutableList.of(new Procedure.Argument("upper", "bigint")));
        assertEquals(
                CallTask.extractParameterValuesInOrder(uppercaseCall, lowercaseProcedure, METADATA, SESSION, NO_PARAMETERS),
                new Object[] {2L});
    }

    @Test
    public void testDelimitedArgumentMatchesExactly()
    {
        Procedure procedure = new Procedure(
                "schema",
                "foo",
                ImmutableList.of(new Procedure.Argument("Mixed", "bigint")));

        Call exactCall = (Call) SQL_PARSER.createStatement("CALL foo(\"Mixed\" => 1)");
        assertEquals(
                CallTask.extractParameterValuesInOrder(exactCall, procedure, METADATA, SESSION, NO_PARAMETERS),
                new Object[] {1L});

        Call mismatchedCall = (Call) SQL_PARSER.createStatement("CALL foo(\"mixed\" => 1)");
        SemanticException exception = expectThrows(
                SemanticException.class,
                () -> CallTask.extractParameterValuesInOrder(mismatchedCall, procedure, METADATA, SESSION, NO_PARAMETERS));
        assertEquals(exception.getMessage(), "line 1:10: Unknown argument name: mixed");
    }

    @Test
    public void testDuplicateUnquotedArgumentNamesUseCanonicalIdentity()
    {
        Call call = (Call) SQL_PARSER.createStatement("CALL foo(UPPER => 1, upper => 2)");
        Procedure procedure = new Procedure(
                "schema",
                "foo",
                ImmutableList.of(new Procedure.Argument("upper", "bigint")));

        SemanticException exception = expectThrows(
                SemanticException.class,
                () -> CallTask.extractParameterValuesInOrder(call, procedure, METADATA, SESSION, NO_PARAMETERS));

        assertEquals(exception.getMessage(), "line 1:22: Duplicate procedure argument: upper");
    }

    @Test
    public void testQuotedAndUnquotedArgumentCanonicalIdentity()
    {
        Procedure procedure = new Procedure(
                "schema",
                "foo",
                ImmutableList.of(new Procedure.Argument("foo", "bigint")));

        Call sameCanonicalName = (Call) SQL_PARSER.createStatement("CALL foo(foo => 1, \"FOO\" => 2)");
        SemanticException sameCanonicalNameException = expectThrows(
                SemanticException.class,
                () -> CallTask.extractParameterValuesInOrder(sameCanonicalName, procedure, METADATA, SESSION, NO_PARAMETERS));
        assertEquals(sameCanonicalNameException.getMessage(), "line 1:20: Duplicate procedure argument: FOO");

        Call differentCanonicalName = (Call) SQL_PARSER.createStatement("CALL foo(foo => 1, \"foo\" => 2)");
        SemanticException differentCanonicalNameException = expectThrows(
                SemanticException.class,
                () -> CallTask.extractParameterValuesInOrder(differentCanonicalName, procedure, METADATA, SESSION, NO_PARAMETERS));
        assertEquals(differentCanonicalNameException.getMessage(), "line 1:20: Duplicate procedure argument: foo");
    }

    @Test
    public void testDelimitedArgumentsDisambiguateDeclarationCase()
    {
        Procedure procedure = new Procedure(
                "schema",
                "foo",
                ImmutableList.of(
                        new Procedure.Argument("foo", "bigint"),
                        new Procedure.Argument("FOO", "bigint")));

        Call delimitedCall = (Call) SQL_PARSER.createStatement("CALL foo(\"foo\" => 1, \"FOO\" => 2)");
        assertEquals(
                CallTask.extractParameterValuesInOrder(delimitedCall, procedure, METADATA, SESSION, NO_PARAMETERS),
                new Object[] {1L, 2L});

        Call unquotedCall = (Call) SQL_PARSER.createStatement("CALL foo(foo => 1)");
        SemanticException exception = expectThrows(
                SemanticException.class,
                () -> CallTask.extractParameterValuesInOrder(unquotedCall, procedure, METADATA, SESSION, NO_PARAMETERS));
        assertEquals(exception.getMessage(), "line 1:10: Ambiguous argument name: foo");
    }

    @Test
    public void testNamedAndPositionalArgumentsCannotBeMixed()
    {
        Call call = (Call) SQL_PARSER.createStatement("CALL foo(1, second => 2)");
        Procedure procedure = new Procedure(
                "schema",
                "foo",
                ImmutableList.of(
                        new Procedure.Argument("first", "bigint"),
                        new Procedure.Argument("second", "bigint")));

        SemanticException exception = expectThrows(
                SemanticException.class,
                () -> CallTask.extractParameterValuesInOrder(call, procedure, METADATA, SESSION, NO_PARAMETERS));

        assertEquals(exception.getMessage(), "line 1:1: Named and positional arguments cannot be mixed");
    }
}
