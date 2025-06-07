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
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.NATIVE_EXECUTION_ENABLED;
import static com.facebook.presto.SystemSessionProperties.NATIVE_EXECUTION_TYPE_REWRITE_ENABLED;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

@Test(singleThreaded = true)
public class TestEnumsWithNativeRewrite
        extends TestEnums
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        try {
            Session session = testSessionBuilder()
                    .setSystemProperty(NATIVE_EXECUTION_ENABLED, "true")
                    .setSystemProperty(NATIVE_EXECUTION_TYPE_REWRITE_ENABLED, "true")
                    .build();
            DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session).setNodeCount(1).build();
            queryRunner.enableTestFunctionNamespaces(ImmutableList.of("test"), ImmutableMap.of());
            queryRunner.getMetadata().getFunctionAndTypeManager().addUserDefinedType(MOOD_ENUM);
            queryRunner.getMetadata().getFunctionAndTypeManager().addUserDefinedType(COUNTRY_ENUM);
            queryRunner.getMetadata().getFunctionAndTypeManager().addUserDefinedType(TEST_ENUM);
            queryRunner.getMetadata().getFunctionAndTypeManager().addUserDefinedType(TEST_BIGINT_ENUM);
            queryRunner.getMetadata().getFunctionAndTypeManager().addUserDefinedType(MARKET_SEGMENT_ENUM);

            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");
            return queryRunner;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    @Test
    public void testEnumFailureCases()
    {
        // TODO: Handle the following cases which should fail but currently not failing:
        // 1. Handle case when key does not exist in the enum type.
        // 2. Handle case when casting from one enum type to another enum type.
        // 3. Handle case where different enum types are compared.

        // Invalid cast
        // assertQueryFails("select cast(7 as test.enum.mood)", ".*No value '7' in enum 'BigintEnum'");
        // assertQueryFails("SELECT cast(test.enum.country.FRANCE as test.enum.testenum)", ".*Cannot cast test.enum.country.* to test.enum.testenum.*");

        // Invalid comparison between different enum types or between enum and base types
        // assertQueryFails("select test.enum.country.US = test.enum.mood.HAPPY", ".* '=' cannot be applied to test.enum.country:VarcharEnum\\(test.enum.country.*\\), test.enum.mood:BigintEnum\\(test.enum.mood.*\\)");
        // assertQueryFails("select test.enum.country.US = test.enum.testenum.TEST2", ".* '=' cannot be applied to test.enum.country:VarcharEnum\\(test.enum.country.*\\), test.enum.mood:BigintEnum\\(test.enum.mood.*\\)");
        // assertQueryFails("select test.enum.country.US > 2", ".* '>' cannot be applied to test.enum.country:VarcharEnum\\(test.enum.country.*\\), integer");
        // assertQueryFails("select test.enum.mood.HAPPY = 3", ".* '=' cannot be applied to test.enum.mood:BigintEnum\\(test.enum.mood.*, integer");
        // assertQueryFails("select test.enum.country.US IN (test.enum.country.CHINA, test.enum.mood.SAD)", ".* All IN list values must be the same type.*");
        // assertQueryFails("select test.enum.country.US IN (test.enum.mood.HAPPY, test.enum.mood.SAD)", ".* IN value and list items must be the same type: test.enum.country");
    }
}
