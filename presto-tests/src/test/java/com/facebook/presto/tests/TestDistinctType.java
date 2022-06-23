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
import com.facebook.presto.common.type.DistinctType;
import com.facebook.presto.common.type.DistinctTypeInfo;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.UserDefinedType;
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

import static com.facebook.presto.common.type.DistinctType.getLowestCommonSuperType;
import static com.facebook.presto.common.type.DistinctType.hasAncestorRelationship;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toSet;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestDistinctType
        extends AbstractTestQueryFramework
{
    // We create 5 distinct types with base type int with following hierarchy:
    //     INT00
    //     /   \
    //   INT10  INT11
    //   /   \
    //  INT20 INT21
    //  /
    // INT30
    private static final UserDefinedType INT00 = createDistinctType("test.dt.int00", Optional.empty(), "integer");
    private static final UserDefinedType INT10 = createDistinctType("test.dt.int10", Optional.of("test.dt.int00"), "integer");
    private static final UserDefinedType INT11 = createDistinctType("test.dt.int11", Optional.of("test.dt.int00"), "integer");
    private static final UserDefinedType INT20 = createDistinctType("test.dt.int20", Optional.of("test.dt.int10"), "integer");
    private static final UserDefinedType INT21 = createDistinctType("test.dt.int21", Optional.of("test.dt.int10"), "integer");
    private static final UserDefinedType INT30 = createDistinctType("test.dt.int30", Optional.of("test.dt.int20"), "integer");

    private static final UserDefinedType INT_ALT = createDistinctType("test.dt.int_alt", Optional.empty(), "integer");
    private static final UserDefinedType INT_NO_ORDER = createDistinctType("test.dt.int_no_order", Optional.empty(), "integer", false);
    private static final UserDefinedType VARCHAR_ALT = createDistinctType("test.dt.varchar_alt", Optional.empty(), "varchar");

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        try {
            Session session = testSessionBuilder().build();
            // Set properties to run queries on worker, and not on the coordinator
            DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session)
                    .setNodeCount(2)
                    .setCoordinatorProperties(ImmutableMap.of("node-scheduler.include-coordinator", "false"))
                    .build();
            // By enabling test function namespace only on coordinator, we ensure that workers
            // don't try to fetch any user defined types
            queryRunner.enableTestFunctionNamespacesOnCoordinators(ImmutableList.of("test"), ImmutableMap.of());
            queryRunner.getMetadata().getFunctionAndTypeManager().addUserDefinedType(INT00);
            queryRunner.getMetadata().getFunctionAndTypeManager().addUserDefinedType(INT10);
            queryRunner.getMetadata().getFunctionAndTypeManager().addUserDefinedType(INT11);
            queryRunner.getMetadata().getFunctionAndTypeManager().addUserDefinedType(INT20);
            queryRunner.getMetadata().getFunctionAndTypeManager().addUserDefinedType(INT21);
            queryRunner.getMetadata().getFunctionAndTypeManager().addUserDefinedType(INT30);
            queryRunner.getMetadata().getFunctionAndTypeManager().addUserDefinedType(INT_ALT);
            queryRunner.getMetadata().getFunctionAndTypeManager().addUserDefinedType(INT_NO_ORDER);
            queryRunner.getMetadata().getFunctionAndTypeManager().addUserDefinedType(VARCHAR_ALT);

            return queryRunner;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testLowestCommonSuperType()
    {
        DistinctType int00 = getDistinctType("test.dt.int00");
        DistinctType int10 = getDistinctType("test.dt.int10");
        DistinctType int11 = getDistinctType("test.dt.int11");
        DistinctType int20 = getDistinctType("test.dt.int20");
        DistinctType int21 = getDistinctType("test.dt.int21");
        DistinctType int30 = getDistinctType("test.dt.int30");
        DistinctType intAlt = getDistinctType("test.dt.int_alt");
        DistinctType varcharAlt = getDistinctType("test.dt.varchar_alt");

        assertEquals(getLowestCommonSuperType(int00, int00).get(), int00);
        assertEquals(getLowestCommonSuperType(int20, int21).get(), int10);
        assertEquals(getLowestCommonSuperType(int21, int20).get(), int10);
        assertEquals(getLowestCommonSuperType(int11, int20).get(), int00);
        assertEquals(getLowestCommonSuperType(int00, int20).get(), int00);
        assertEquals(getLowestCommonSuperType(int20, int30).get(), int20);
        assertEquals(getLowestCommonSuperType(int30, int20).get(), int20);
        assertEquals(getLowestCommonSuperType(int21, int30).get(), int10);

        assertEquals(getLowestCommonSuperType(int00, intAlt), Optional.empty());
        assertEquals(getLowestCommonSuperType(int00, varcharAlt), Optional.empty());
    }

    @Test
    public void testAncestorRelationship()
    {
        DistinctType int00 = getDistinctType("test.dt.int00");
        DistinctType int10 = getDistinctType("test.dt.int10");
        DistinctType int11 = getDistinctType("test.dt.int11");
        DistinctType int20 = getDistinctType("test.dt.int20");
        DistinctType int21 = getDistinctType("test.dt.int21");
        DistinctType int30 = getDistinctType("test.dt.int30");
        DistinctType intAlt = getDistinctType("test.dt.int_alt");
        DistinctType varcharAlt = getDistinctType("test.dt.varchar_alt");

        assertTrue(hasAncestorRelationship(int00, int00));
        assertTrue(hasAncestorRelationship(int00, int20));
        assertTrue(hasAncestorRelationship(int20, int00));
        assertTrue(hasAncestorRelationship(int10, int20));
        assertTrue(hasAncestorRelationship(int21, int10));
        assertTrue(hasAncestorRelationship(int20, int00));
        assertTrue(hasAncestorRelationship(int20, int30));

        assertFalse(hasAncestorRelationship(int20, int21));
        assertFalse(hasAncestorRelationship(int11, int20));
        assertFalse(hasAncestorRelationship(int30, int21));

        assertFalse(hasAncestorRelationship(int00, intAlt));
        assertFalse(hasAncestorRelationship(int00, varcharAlt));
    }

    @Test
    public void testSerdeAndLazyLoading()
    {
        DistinctType int00 = getDistinctType("test.dt.int00");
        DistinctType int10 = getDistinctType("test.dt.int10");
        DistinctType int11 = getDistinctType("test.dt.int11");
        DistinctType int20 = getDistinctType("test.dt.int20");
        DistinctType int21 = getDistinctType("test.dt.int21");
        DistinctType int30 = getDistinctType("test.dt.int30");
        DistinctType intAlt = getDistinctType("test.dt.int_alt");
        String int00Signature = "test.dt.int00:DistinctType(test.dt.int00{integer, true, null, []})";
        String int20Signature = "test.dt.int20:DistinctType(test.dt.int20{integer, true, test.dt.int10, []})";

        assertEquals(int00.getTypeSignature().toString(), "test.dt.int00:DistinctType(test.dt.int00{integer, true, null, []})");
        assertEquals(int20.getTypeSignature().toString(), "test.dt.int20:DistinctType(test.dt.int20{integer, true, test.dt.int10, []})");
        assertEquals(getDistinctType(parseTypeSignature(int00Signature)), int00);
        assertEquals(getDistinctType(parseTypeSignature(int20Signature)), int20);

        assertEquals(getLowestCommonSuperType(int20, int21).get(), int10);
        assertEquals(int20.getTypeSignature().toString(), "test.dt.int20:DistinctType(test.dt.int20{integer, true, test.dt.int00, [test.dt.int10]})");

        assertEquals(getLowestCommonSuperType(int30, int21).get(), int10);
        assertEquals(int30.getTypeSignature().toString(), "test.dt.int30:DistinctType(test.dt.int30{integer, true, test.dt.int10, [test.dt.int20]})");

        assertEquals(getLowestCommonSuperType(int30, int11).get(), int00);
        assertEquals(int30.getTypeSignature().toString(), "test.dt.int30:DistinctType(test.dt.int30{integer, true, test.dt.int00, [test.dt.int20, test.dt.int10]})");

        assertEquals(getLowestCommonSuperType(int30, intAlt), Optional.empty());
        assertEquals(int30.getTypeSignature().toString(), "test.dt.int30:DistinctType(test.dt.int30{integer, true, null, [test.dt.int20, test.dt.int10, test.dt.int00]})");
    }

    @Test
    public void testCasts()
    {
        assertSingleResultFromValues("SELECT CAST(CAST(x AS test.dt.int00) AS INTEGER)", "1", 1);
        assertSingleResultFromValues("SELECT CAST(CAST(x AS test.dt.varchar_alt) AS VARCHAR)", "CAST('name' AS VARCHAR)", "name");
        assertQueryFails("SELECT CAST(CAST(1 AS test.dt.int00) AS VARCHAR)", "Cannot cast test.dt.int00 to varchar");
        assertQueryFails("SELECT CAST(CAST(1 AS BIGINT) AS test.dt.int00)", "Cannot cast bigint to test.dt.int00");
        assertQueryFails("SELECT CAST(CAST(1 AS test.dt.int00) AS BIGINT)", "Cannot cast test.dt.int00 to bigint");

        assertSingleResultFromValues("SELECT CAST(CAST(x AS test.dt.int00) AS test.dt.int00)", "1", 1);
        assertSingleResultFromValues("SELECT CAST(CAST(x AS test.dt.int00) AS test.dt.int30)", "1", 1);
        assertSingleResultFromValues("SELECT CAST(CAST(x AS test.dt.int10) AS test.dt.int30)", "1", 1);
        assertSingleResultFromValues("SELECT CAST(CAST(x AS test.dt.int30) AS test.dt.int00)", "1", 1);
        assertQueryFails("SELECT CAST(CAST(1 AS test.dt.int30) AS test.dt.int11)", "Cannot cast test.dt.int30 to test.dt.int11");
        assertQueryFails("SELECT CAST(CAST(1 AS test.dt.int20) AS test.dt.int21)", "Cannot cast test.dt.int20 to test.dt.int21");

        assertSingleResultFromValues("SELECT CAST(ARRAY[ARRAY[CAST(x AS test.dt.int00)]] AS ARRAY<ARRAY<test.dt.int30>>)", "1", ImmutableList.of(ImmutableList.of(1)));
        assertSingleResultFromValues("SELECT CAST(ARRAY[ARRAY[CAST(x AS test.dt.int30)]] AS ARRAY<ARRAY<test.dt.int00>>)", "1", ImmutableList.of(ImmutableList.of(1)));
        assertSingleResultFromValues("SELECT CAST(ARRAY[CAST(x AS test.dt.int30)] AS ARRAY<test.dt.int00>)", "1", ImmutableList.of(1));
        assertSingleResultFromValues("SELECT CAST(ARRAY[CAST(x AS test.dt.int00)] AS ARRAY<test.dt.int30>)", "1", ImmutableList.of(1));
        assertSingleResultFromValues("SELECT ARRAY_INTERSECT(ARRAY[ARRAY[CAST(x AS test.dt.int30)]], ARRAY[ARRAY[CAST(1 AS test.dt.int11)]])", "1", ImmutableList.of(ImmutableList.of(1)));
    }

    @Test
    public void testDistinctFrom()
    {
        assertSingleResultFromValues("SELECT CAST(1 as test.dt.int00) IS DISTINCT FROM CAST(x as test.dt.int00)", "1", false);
        assertSingleResultFromValues("SELECT CAST(1 as test.dt.int00) IS DISTINCT FROM CAST(x as test.dt.int00)", "2", true);
        assertSingleResultFromValues("SELECT CAST(1 as test.dt.int00) IS DISTINCT FROM CAST(x as test.dt.int30)", "1", false);
        assertSingleResultFromValues("SELECT NULL IS DISTINCT FROM CAST(x as test.dt.int00)", "1", true);
        assertSingleResultFromValues("SELECT CAST(x as test.dt.int00) IS DISTINCT FROM NULL", "1", true);

        assertSingleResultFromValues("SELECT ARRAY[ROW(CAST(1 as test.dt.int00))] IS DISTINCT FROM ARRAY[ROW(CAST(x as test.dt.int00))]", "1", false);
        assertQueryFails("SELECT CAST(1 as test.dt.int00) IS DISTINCT FROM CAST(1 as test.dt.int_alt)", ".*cannot be applied to test.dt.int00.*");
        assertQueryFails("SELECT CAST(1 as test.dt.int00) IS DISTINCT FROM 1", ".*cannot be applied to test.dt.int00.*");
    }

    @Test
    public void testHash()
    {
        assertQueryResultUnordered("SELECT DISTINCT x FROM (VALUES CAST(1 as test.dt.int00), CAST(1 as test.dt.int00), CAST(2 as test.dt.int00)) t(x)", ImmutableList.of(ImmutableList.of(1), ImmutableList.of(2)));
        assertSingleResult("SELECT APPROX_DISTINCT(x) FROM (VALUES CAST(1 as test.dt.int00), CAST(1 as test.dt.int00), CAST(2 as test.dt.int00)) t(x)", 2L);
        assertQueryResultUnordered("SELECT x FROM (VALUES CAST(1 as test.dt.int00), CAST(1 as test.dt.int00), CAST(2 as test.dt.int00)) t(x) GROUP BY x", ImmutableList.of(ImmutableList.of(1), ImmutableList.of(2)));

        assertQueryResultUnordered("SELECT DISTINCT x FROM (VALUES ARRAY[CAST(1 as test.dt.int00)], ARRAY[CAST(1 as test.dt.int00)], ARRAY[CAST(2 as test.dt.int00)]) t(x)", ImmutableList.of(ImmutableList.of(ImmutableList.of(1)), ImmutableList.of(ImmutableList.of(2))));
    }

    @Test
    public void testNotOrderable()
    {
        assertQueryFails(
                "SELECT CAST(1 AS test.dt.int_no_order) < CAST(2 AS test.dt.int_no_order)",
                "Type test.dt.int_no_order\\(integer\\) does not allow ordering");
        assertQueryFails(
                "SELECT CAST(1 AS test.dt.int_no_order) <= CAST(2 AS test.dt.int_no_order)",
                "Type test.dt.int_no_order\\(integer\\) does not allow ordering");
        assertQueryFails(
                "SELECT CAST(1 AS test.dt.int_no_order) > CAST(2 AS test.dt.int_no_order)",
                "Type test.dt.int_no_order\\(integer\\) does not allow ordering");
        assertQueryFails(
                "SELECT CAST(1 AS test.dt.int_no_order) >= CAST(2 AS test.dt.int_no_order)",
                "Type test.dt.int_no_order\\(integer\\) does not allow ordering");
        assertQueryFails(
                "SELECT CAST(2 AS test.dt.int_no_order) BETWEEN CAST(1 AS test.dt.int_no_order) AND CAST(3 AS test.dt.int_no_order)",
                "Type test.dt.int_no_order\\(integer\\) does not allow ordering");

        assertQueryFails(
                "SELECT * FROM (" +
                        "    VALUES" +
                        "        (CAST(1 AS test.dt.int_no_order))," +
                        "        (CAST(2 AS test.dt.int_no_order))" +
                        ") AS t (id)" +
                        "order by 1",
                ".*Type test.dt.int_no_order.* is not orderable.*");
    }

    @Test
    public void testComparison()
    {
        assertSingleResultFromValues("SELECT CAST(1 as test.dt.int00) = CAST(x as test.dt.int00)", "1", true);
        assertSingleResultFromValues("SELECT CAST(1 as test.dt.int30) = CAST(x as test.dt.int11)", "1", true);
        assertSingleResultFromValues("SELECT CAST(1 as test.dt.int30) = CAST(x as test.dt.int11)", "2", false);
        assertSingleResultFromValues("SELECT CAST(x as test.dt.int00) = NULL", "1", null);
        assertSingleResultFromValues("SELECT ARRAY[ROW(CAST(1 as test.dt.int30))] = ARRAY[ROW(CAST(x as test.dt.int21))]", "1", true);

        assertSingleResultFromValues("SELECT CAST(1 as test.dt.int00) <> CAST(x as test.dt.int00)", "1", false);
        assertSingleResultFromValues("SELECT CAST(1 as test.dt.int30) <> CAST(x as test.dt.int11)", "1", false);
        assertSingleResultFromValues("SELECT CAST(1 as test.dt.int30) <> CAST(x as test.dt.int11)", "2", true);
        assertSingleResultFromValues("SELECT CAST(x as test.dt.int00) <> NULL", "1", null);
        assertSingleResultFromValues("SELECT ARRAY[ROW(CAST(1 as test.dt.int30))] <> ARRAY[ROW(CAST(x as test.dt.int21))]", "1", false);

        assertSingleResultFromValues("SELECT CAST(1 as test.dt.int00) < CAST(x as test.dt.int00)", "1", false);
        assertSingleResultFromValues("SELECT CAST(2 as test.dt.int30) < CAST(x as test.dt.int11)", "1", false);
        assertSingleResultFromValues("SELECT CAST(1 as test.dt.int30) < CAST(x as test.dt.int11)", "2", true);
        assertSingleResultFromValues("SELECT CAST(x as test.dt.int00) < NULL", "1", null);
        assertSingleResultFromValues("SELECT ARRAY[ROW(CAST(1 as test.dt.int30))] < ARRAY[ROW(CAST(x as test.dt.int21))]", "2", true);

        assertSingleResultFromValues("SELECT CAST(1 as test.dt.int00) <= CAST(x as test.dt.int00)", "1", true);
        assertSingleResultFromValues("SELECT CAST(2 as test.dt.int30) <= CAST(x as test.dt.int11)", "1", false);
        assertSingleResultFromValues("SELECT CAST(1 as test.dt.int30) <= CAST(x as test.dt.int11)", "2", true);
        assertSingleResultFromValues("SELECT CAST(x as test.dt.int00) <= NULL", "1", null);
        assertSingleResultFromValues("SELECT ARRAY[ROW(CAST(1 as test.dt.int30))] <= ARRAY[ROW(CAST(x as test.dt.int21))]", "1", true);

        assertSingleResultFromValues("SELECT CAST(1 as test.dt.int00) > CAST(x as test.dt.int00)", "1", false);
        assertSingleResultFromValues("SELECT CAST(2 as test.dt.int30) > CAST(x as test.dt.int11)", "1", true);
        assertSingleResultFromValues("SELECT CAST(1 as test.dt.int30) > CAST(x as test.dt.int11)", "2", false);
        assertSingleResultFromValues("SELECT CAST(x as test.dt.int00) > NULL", "1", null);
        assertSingleResultFromValues("SELECT ARRAY[ROW(CAST(1 as test.dt.int30))] > ARRAY[ROW(CAST(x as test.dt.int21))]", "1", false);

        assertSingleResultFromValues("SELECT CAST(1 as test.dt.int00) >= CAST(x as test.dt.int00)", "1", true);
        assertSingleResultFromValues("SELECT CAST(1 as test.dt.int30) >= CAST(x as test.dt.int11)", "1", true);
        assertSingleResultFromValues("SELECT CAST(1 as test.dt.int30) >= CAST(x as test.dt.int11)", "2", false);
        assertSingleResultFromValues("SELECT CAST(x as test.dt.int00) >= NULL", "1", null);
        assertSingleResultFromValues("SELECT ARRAY[ROW(CAST(2 as test.dt.int30))] >= ARRAY[ROW(CAST(x as test.dt.int21))]", "1", true);

        assertSingleResultFromValues("SELECT CAST(x as test.dt.int00) BETWEEN CAST(1 as test.dt.int11) AND CAST(3 as test.dt.int30)", "2", true);
        assertSingleResultFromValues("SELECT NULL BETWEEN CAST(x as test.dt.int11) AND CAST(3 as test.dt.int30)", "2", null);

        assertQueryFails("SELECT CAST(1 as test.dt.int00) = CAST(1 as test.dt.int_alt)", ".*cannot be applied to test.dt.int00.*");
        assertQueryFails("SELECT CAST(1 as test.dt.int00) = 1", ".*cannot be applied to test.dt.int00.*");
        assertQueryFails("SELECT CAST(1 as test.dt.int00) <> CAST(1 as test.dt.int_alt)", ".*cannot be applied to test.dt.int00.*");
        assertQueryFails("SELECT CAST(1 as test.dt.int00) <> 1", ".*cannot be applied to test.dt.int00.*");
        assertQueryFails("SELECT CAST(1 as test.dt.int00) < CAST(1 as test.dt.int_alt)", ".*cannot be applied to test.dt.int00.*");
        assertQueryFails("SELECT CAST(1 as test.dt.int00) < 1", ".*cannot be applied to test.dt.int00.*");
        assertQueryFails("SELECT CAST(1 as test.dt.int00) <= CAST(1 as test.dt.int_alt)", ".*cannot be applied to test.dt.int00.*");
        assertQueryFails("SELECT CAST(1 as test.dt.int00) <= 1", ".*cannot be applied to test.dt.int00.*");
        assertQueryFails("SELECT CAST(1 as test.dt.int00) > CAST(1 as test.dt.int_alt)", ".*cannot be applied to test.dt.int00.*");
        assertQueryFails("SELECT CAST(1 as test.dt.int00) > 1", ".*cannot be applied to test.dt.int00.*");
        assertQueryFails("SELECT CAST(1 as test.dt.int00) >= CAST(1 as test.dt.int_alt)", ".*cannot be applied to test.dt.int00.*");
        assertQueryFails("SELECT CAST(1 as test.dt.int00) >= 1", ".*cannot be applied to test.dt.int00.*");
        assertQueryFails("SELECT 2 BETWEEN CAST(1 as test.dt.int11) AND CAST(3 as test.dt.int30)", ".*Cannot check if integer is BETWEEN.*");
        assertQueryFails("SELECT CAST(2 as test.dt.int_alt) BETWEEN CAST(1 as test.dt.int11) AND CAST(3 as test.dt.int30)", ".*Cannot check if test.dt.int_alt.*");
    }

    private void assertQueryResultUnordered(@Language("SQL") String query, List<List<Object>> expectedRows)
    {
        MaterializedResult rows = computeActual(query);
        assertEquals(
                ImmutableSet.copyOf(rows.getMaterializedRows()),
                expectedRows.stream().map(row -> new MaterializedRow(1, row)).collect(toSet()));
    }

    private void assertSingleResult(@Language("SQL") String query, Object expectedResult)
    {
        assertQueryResultUnordered(query, singletonList(singletonList(expectedResult)));
    }

    // We select from values in the tests, so query runs on the worker node, and we can test it end to end.
    private void assertSingleResultFromValues(@Language("SQL") String query, String valuesInput, Object expectedResult)
    {
        assertQueryResultUnordered(format("%s FROM (VALUES %s) t(x)", query, valuesInput), singletonList(singletonList(expectedResult)));
    }

    protected DistinctType getDistinctType(String name)
    {
        return getDistinctType(new TypeSignature(QualifiedObjectName.valueOf(name)));
    }

    protected DistinctType getDistinctType(TypeSignature typeSignature)
    {
        return (DistinctType) getQueryRunner().getMetadata().getFunctionAndTypeManager().getType(typeSignature);
    }

    private static UserDefinedType createDistinctType(String name, Optional<String> parent, String baseType)
    {
        return createDistinctType(name, parent, baseType, true);
    }

    private static UserDefinedType createDistinctType(String name, Optional<String> parent, String baseType, boolean isOrderable)
    {
        return new UserDefinedType(
                QualifiedObjectName.valueOf(name),
                new TypeSignature(
                        new DistinctTypeInfo(
                                QualifiedObjectName.valueOf(name),
                                parseTypeSignature(baseType),
                                parent.map(QualifiedObjectName::valueOf),
                                isOrderable)));
    }
}
