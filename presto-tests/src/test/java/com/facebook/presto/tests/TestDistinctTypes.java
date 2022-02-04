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

import com.facebook.presto.Session;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.type.DistinctTypeInfo;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.UserDefinedType;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.common.type.StandardTypes.BOOLEAN;
import static com.facebook.presto.common.type.StandardTypes.DISTINCT_TYPE;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.metadata.FunctionExtractor.extractFunctions;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.util.Collections.singletonList;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestDistinctTypes
        extends AbstractTestQueryFramework
{
    private static final UserDefinedType USER_ID = new UserDefinedType(
            QualifiedObjectName.valueOf("test.dt.user_id"),
            new TypeSignature(
                    new DistinctTypeInfo(
                            QualifiedObjectName.valueOf("test.dt.user_id"),
                            parseTypeSignature("integer"),
                            Optional.empty(),
                            true)));

    private static final UserDefinedType ADMIN_ID = new UserDefinedType(
            QualifiedObjectName.valueOf("test.dt.admin_id"),
            new TypeSignature(
                    new DistinctTypeInfo(
                            QualifiedObjectName.valueOf("test.dt.admin_id"),
                            parseTypeSignature("integer"),
                            Optional.of(QualifiedObjectName.valueOf("test.dt.user_id")),
                            true)));

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        try {
            Session session = testSessionBuilder().build();
            DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session)
                    .setNodeCount(2)
                    .setCoordinatorProperties(ImmutableMap.of("node-scheduler.include-coordinator", "false"))
                    .build();
            queryRunner.enableTestFunctionNamespaces(ImmutableList.of("test"), ImmutableMap.of());
            queryRunner.getMetadata().getFunctionAndTypeManager().addUserDefinedType(USER_ID);
            queryRunner.getMetadata().getFunctionAndTypeManager().addUserDefinedType(ADMIN_ID);
            queryRunner.registerBuiltInFunctions(extractFunctions(TestDistinctTypes.class));

            return queryRunner;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void assertQueryResultUnordered(@Language("SQL") String query, List<List<Object>> expectedRows)
    {
        MaterializedResult rows = computeActual(query);
        assertEquals(
                ImmutableSet.copyOf(rows.getMaterializedRows()),
                expectedRows.stream().map(row -> new MaterializedRow(1, row)).collect(Collectors.toSet()));
    }

    private void assertSingleValue(@Language("SQL") String query, Object expectedResult)
    {
        assertQueryResultUnordered(query, singletonList(singletonList(expectedResult)));
    }

    @Test
    public void testDistinctCasts()
    {
        // Using values ensures that queries are run on workers
        assertSingleValue("SELECT CAST(x as test.dt.user_id) = CAST(1 as test.dt.admin_id) FROM (VALUES 1) t(x)", true);
        assertSingleValue("SELECT CAST(1 as test.dt.user_id) = CAST(x as test.dt.admin_id) FROM (VALUES 2) t(x)", false);

        assertSingleValue("SELECT CAST(x AS test.dt.user_id) = CAST(x as test.dt.user_id) FROM (VALUES 1) t(x)", true);
        assertSingleValue("SELECT CAST(CAST(x AS test.dt.user_id) AS integer) FROM (VALUES 1) t(x)", 1);

        assertQueryFails("SELECT CAST(CAST(1 AS test.dt.user_id) AS varchar)", "Cannot cast test.dt.user_id to varchar");
        assertQueryFails("SELECT CAST(CAST(1 AS BIGINT) AS test.dt.user_id)", "Cannot cast bigint to test.dt.user_id");

        assertSingleValue("SELECT cast(array[cast(x as test.dt.admin_id)] as array(test.dt.user_id)) FROM (VALUES 1) t(x)", singletonList(1));
        assertSingleValue("SELECT array[cast(x as test.dt.admin_id)] = array[cast(1 as test.dt.user_id)] FROM (VALUES 1) t(x)", true);

        assertSingleValue("SELECT good_user(cast(x as test.dt.user_id)) FROM (VALUES 1) t(x)", true);
        assertSingleValue("SELECT good_user(cast(x as test.dt.admin_id)) FROM (VALUES 1) t(x)", true);
        assertQueryFails("SELECT good_admin(cast(1 as test.dt.user_id))", ".*Unable to find matching typeName.*");
    }

    @ScalarFunction("good_user")
    @TypeParameter(value = "T", boundedBy = DISTINCT_TYPE, typeName = "test.dt.user_id")
    @SqlType(BOOLEAN)
    public static boolean goodUser(@SqlType("T") long value)
    {
        return value == 1;
    }

    @ScalarFunction("good_admin")
    @TypeParameter(value = "T", boundedBy = DISTINCT_TYPE, typeName = "test.dt.admin_id")
    @SqlType(BOOLEAN)
    public static boolean goodAdmin(@SqlType("T") long value)
    {
        return value == 1;
    }
}
