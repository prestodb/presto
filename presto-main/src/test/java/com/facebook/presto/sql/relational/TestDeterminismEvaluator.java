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

import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.common.function.OperatorType.LESS_THAN;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.relational.Expressions.field;
import static java.util.Collections.singletonList;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestDeterminismEvaluator
{
    @Test
    public void testDeterminismEvaluator()
    {
        FunctionAndTypeManager functionAndTypeManager = createTestMetadataManager().getFunctionAndTypeManager();
        RowExpressionDeterminismEvaluator determinismEvaluator = new RowExpressionDeterminismEvaluator(functionAndTypeManager);

        CallExpression random = new CallExpression(
                "random",
                functionAndTypeManager.lookupFunction("random", fromTypes(BIGINT)),
                BIGINT,
                singletonList(constant(10L, BIGINT)));
        assertFalse(determinismEvaluator.isDeterministic(random));

        InputReferenceExpression col0 = field(0, BIGINT);
        FunctionHandle lessThan = functionAndTypeManager.resolveOperator(LESS_THAN, fromTypes(BIGINT, BIGINT));

        CallExpression lessThanExpression = new CallExpression(LESS_THAN.name(), lessThan, BOOLEAN, ImmutableList.of(col0, constant(10L, BIGINT)));
        assertTrue(determinismEvaluator.isDeterministic(lessThanExpression));

        CallExpression lessThanRandomExpression = new CallExpression(LESS_THAN.name(), lessThan, BOOLEAN, ImmutableList.of(col0, random));
        assertFalse(determinismEvaluator.isDeterministic(lessThanRandomExpression));
    }
}
