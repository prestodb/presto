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
package io.prestosql.sql.planner;

import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.FunctionCall;
import io.prestosql.sql.tree.Identifier;
import io.prestosql.sql.tree.QualifiedName;
import org.testng.annotations.Test;

import java.util.Arrays;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestDeterminismEvaluator
{
    @Test
    public void testSanity()
    {
        assertFalse(DeterminismEvaluator.isDeterministic(function("rand")));
        assertFalse(DeterminismEvaluator.isDeterministic(function("random")));
        assertFalse(DeterminismEvaluator.isDeterministic(function("shuffle")));
        assertTrue(DeterminismEvaluator.isDeterministic(function("abs", input("symbol"))));
        assertFalse(DeterminismEvaluator.isDeterministic(function("abs", function("rand"))));
        assertTrue(DeterminismEvaluator.isDeterministic(function("abs", function("abs", input("symbol")))));
    }

    private static FunctionCall function(String name, Expression... inputs)
    {
        return new FunctionCall(QualifiedName.of(name), Arrays.asList(inputs));
    }

    private static Identifier input(String symbol)
    {
        return new Identifier(symbol);
    }
}
