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
package io.prestosql.sql.planner.plan;

import com.google.common.collect.ImmutableCollection;
import io.prestosql.sql.planner.Symbol;
import org.testng.annotations.Test;

import static io.prestosql.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static org.testng.Assert.assertTrue;

public class TestAssingments
{
    private final Assignments assignments = Assignments.of(new Symbol("test"), TRUE_LITERAL);

    @Test
    public void testOutputsImmutable()
    {
        assertTrue(assignments.getOutputs() instanceof ImmutableCollection);
    }

    @Test
    public void testOutputsMemoized()
    {
        assertTrue(assignments.getOutputs() == assignments.getOutputs());
    }
}
