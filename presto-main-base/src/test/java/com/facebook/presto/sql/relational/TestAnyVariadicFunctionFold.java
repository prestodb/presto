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
package com.facebook.presto.sql.relational;

import com.facebook.presto.Session;
import com.facebook.presto.functionNamespace.FunctionNamespaceManagerPlugin;
import com.facebook.presto.functionNamespace.json.JsonFileBasedFunctionNamespaceManagerFactory;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.MaterializedResult;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import org.testng.annotations.Test;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.SystemSessionProperties.FIELD_NAMES_IN_JSON_CAST_ENABLED;
import static org.testng.Assert.assertEquals;

/**
 * End-to-end coverage for the internal {@code __ANY__} variable-arity tail: a call to a facing
 * {@code f(varchar, __ANY__)} function binds (ANY gate), the RowExpression translator folds the
 * trailing mixed-type arguments into a single {@code CAST(ROW(...) AS JSON)} and dispatches to the
 * concrete {@code f(varchar, json)} overload, whose SQL body inlines and executes.
 */
public class TestAnyVariadicFunctionFold
{
    @Test
    public void testAnyVariadicFoldsToJsonAndExecutes()
    {
        // Emit the folded ROW as a positional JSON array (not a field-named object) so the backend sees
        // the trailing arguments as [1, "b", true].
        Session session = Session.builder(TEST_SESSION)
                .setSystemProperty(FIELD_NAMES_IN_JSON_CAST_ENABLED, "false")
                .build();
        try (LocalQueryRunner queryRunner = new LocalQueryRunner(session)) {
            queryRunner.installPlugin(new FunctionNamespaceManagerPlugin());
            queryRunner.loadFunctionNamespaceManager(
                    JsonFileBasedFunctionNamespaceManagerFactory.NAME,
                    "json",
                    ImmutableMap.of(
                            "supported-function-languages", "SQL",
                            "function-implementation-type", "SQL",
                            "json-based-function-manager.path-to-function-definition",
                            Resources.getResource("json_udf_any_variadic_executable.json").getFile()));

            // Three trailing mixed-type args (bigint, varchar, boolean) fold into a JSON array of length 3.
            MaterializedResult result = queryRunner.execute("SELECT json.test_schema.count_any('fmt', 1, 'b', true)");
            assertEquals(result.getOnlyValue(), 3L);
        }
    }
}
