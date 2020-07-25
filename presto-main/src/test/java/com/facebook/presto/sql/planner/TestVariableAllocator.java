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
package com.facebook.presto.sql.planner;

import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.Set;

import static org.testng.Assert.assertEquals;

public class TestVariableAllocator
{
    @Test
    public void testUnique()
    {
        PlanVariableAllocator allocator = new PlanVariableAllocator();
        Set<VariableReferenceExpression> variables = ImmutableSet.<VariableReferenceExpression>builder()
                .add(allocator.newVariable("foo_1_0", BigintType.BIGINT))
                .add(allocator.newVariable("foo", BigintType.BIGINT))
                .add(allocator.newVariable("foo", BigintType.BIGINT))
                .add(allocator.newVariable("foo", BigintType.BIGINT))
                .build();

        assertEquals(variables.size(), 4);
    }
}
