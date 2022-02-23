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
import com.facebook.presto.testing.QueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.common.type.DistinctType.getLowestCommonSuperType;
import static com.facebook.presto.common.type.DistinctType.hasAncestorRelationship;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
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
    private static final UserDefinedType VARCHAR_ALT = createDistinctType("test.dt.varchar_alt", Optional.empty(), "varchar");

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        try {
            Session session = testSessionBuilder().build();
            DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session).setNodeCount(1).build();
            queryRunner.enableTestFunctionNamespaces(ImmutableList.of("test"), ImmutableMap.of());
            queryRunner.getMetadata().getFunctionAndTypeManager().addUserDefinedType(INT00);
            queryRunner.getMetadata().getFunctionAndTypeManager().addUserDefinedType(INT10);
            queryRunner.getMetadata().getFunctionAndTypeManager().addUserDefinedType(INT11);
            queryRunner.getMetadata().getFunctionAndTypeManager().addUserDefinedType(INT20);
            queryRunner.getMetadata().getFunctionAndTypeManager().addUserDefinedType(INT21);
            queryRunner.getMetadata().getFunctionAndTypeManager().addUserDefinedType(INT30);
            queryRunner.getMetadata().getFunctionAndTypeManager().addUserDefinedType(INT_ALT);
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
        return new UserDefinedType(
                QualifiedObjectName.valueOf(name),
                new TypeSignature(
                        new DistinctTypeInfo(
                                QualifiedObjectName.valueOf(name),
                                parseTypeSignature(baseType),
                                parent.map(QualifiedObjectName::valueOf),
                                true)));
    }
}
