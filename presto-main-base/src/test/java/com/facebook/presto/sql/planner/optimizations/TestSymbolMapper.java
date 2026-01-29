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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;

public class TestSymbolMapper
{
    @Test
    public void testBasicVariableMapping()
    {
        VariableReferenceExpression varA = new VariableReferenceExpression(Optional.empty(), "a", BigintType.BIGINT);
        VariableReferenceExpression varB = new VariableReferenceExpression(Optional.empty(), "b", BigintType.BIGINT);

        SymbolMapper mapper = new SymbolMapper(ImmutableMap.of(varA, varB), WarningCollector.NOOP);

        VariableReferenceExpression mapped = mapper.map(varA);
        assertEquals(mapped.getName(), "b");
        assertEquals(mapped.getType(), BigintType.BIGINT);
    }

    @Test
    public void testUnmappedVariableReturnsSameInstance()
    {
        VariableReferenceExpression varA = new VariableReferenceExpression(Optional.empty(), "a", BigintType.BIGINT);
        VariableReferenceExpression varB = new VariableReferenceExpression(Optional.empty(), "b", BigintType.BIGINT);

        SymbolMapper mapper = new SymbolMapper(ImmutableMap.of(varA, varB), WarningCollector.NOOP);

        VariableReferenceExpression varC = new VariableReferenceExpression(Optional.empty(), "c", BigintType.BIGINT);
        VariableReferenceExpression mapped = mapper.map(varC);
        assertSame(mapped, varC);
    }

    @Test
    public void testTransitiveMapping()
    {
        VariableReferenceExpression varA = new VariableReferenceExpression(Optional.empty(), "a", BigintType.BIGINT);
        VariableReferenceExpression varB = new VariableReferenceExpression(Optional.empty(), "b", BigintType.BIGINT);
        VariableReferenceExpression varC = new VariableReferenceExpression(Optional.empty(), "c", BigintType.BIGINT);

        // A -> B -> C should resolve A to C
        Map<VariableReferenceExpression, VariableReferenceExpression> mapping = new HashMap<>();
        mapping.put(varA, varB);
        mapping.put(varB, varC);

        SymbolMapper mapper = new SymbolMapper(mapping, WarningCollector.NOOP);

        VariableReferenceExpression mapped = mapper.map(varA);
        assertEquals(mapped.getName(), "c");
    }

    @Test
    public void testMapList()
    {
        VariableReferenceExpression varA = new VariableReferenceExpression(Optional.empty(), "a", BigintType.BIGINT);
        VariableReferenceExpression varB = new VariableReferenceExpression(Optional.empty(), "b", BigintType.BIGINT);
        VariableReferenceExpression varX = new VariableReferenceExpression(Optional.empty(), "x", VarcharType.VARCHAR);
        VariableReferenceExpression varY = new VariableReferenceExpression(Optional.empty(), "y", VarcharType.VARCHAR);

        Map<VariableReferenceExpression, VariableReferenceExpression> mapping = ImmutableMap.of(varA, varB, varX, varY);
        SymbolMapper mapper = new SymbolMapper(mapping, WarningCollector.NOOP);

        List<VariableReferenceExpression> result = mapper.map(List.of(varA, varX));
        assertEquals(result.size(), 2);
        assertEquals(result.get(0).getName(), "b");
        assertEquals(result.get(1).getName(), "y");
    }

    @Test
    public void testBuilderPattern()
    {
        VariableReferenceExpression varA = new VariableReferenceExpression(Optional.empty(), "a", BigintType.BIGINT);
        VariableReferenceExpression varB = new VariableReferenceExpression(Optional.empty(), "b", BigintType.BIGINT);

        SymbolMapper.Builder builder = SymbolMapper.builder();
        builder.put(varA, varB);
        SymbolMapper mapper = builder.build();

        VariableReferenceExpression mapped = mapper.map(varA);
        assertEquals(mapped.getName(), "b");
    }
}
