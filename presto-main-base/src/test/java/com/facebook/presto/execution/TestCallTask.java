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
    public void testCanonicalArgumentNamesMatchProcedure()
    {
        Call call = (Call) SQL_PARSER.createStatement("CALL foo(UPPER => 1, \"Mixed\" => 2)");
        Procedure procedure = new Procedure(
                "schema",
                "foo",
                ImmutableList.of(
                        new Procedure.Argument("upper", "bigint"),
                        new Procedure.Argument("Mixed", "bigint")));

        assertEquals(
                CallTask.extractParameterValuesInOrder(call, procedure, METADATA, SESSION, NO_PARAMETERS),
                new Object[] {1L, 2L});
    }

    @Test
    public void testDuplicateArgumentNamesUseCanonicalIdentity()
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
}
