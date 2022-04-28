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
package com.facebook.presto.orc.metadata;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.orc.OrcTester;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Set;

import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.orc.metadata.OrcType.mapColumnToNode;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestOrcType
{
    @Test
    public void testMapColumnToNodeSimpleTypes()
    {
        List<String> columnNames = ImmutableList.of("f1", "f2", "f3");
        List<Type> columnTypes = ImmutableList.of(VARCHAR, VARCHAR, VARCHAR);
        List<OrcType> orcTypes = OrcType.createOrcRowType(0, columnNames, columnTypes);
        Set<Integer> actual = mapColumnToNode(ImmutableSet.of(0, 2), orcTypes);
        Set<Integer> expected = ImmutableSet.of(1, 3);
        assertEquals(actual, expected);
    }

    @Test
    public void testMapColumnToNodeNestedTypes()
    {
        List<String> columnNames = ImmutableList.of("f1", "f2", "f3");
        List<Type> columnTypes = ImmutableList.of(VARCHAR, OrcTester.mapType(VARCHAR, VARCHAR), VARCHAR);
        List<OrcType> orcTypes = OrcType.createOrcRowType(0, columnNames, columnTypes);

        Set<Integer> actual = mapColumnToNode(ImmutableSet.of(0, 2), orcTypes);
        Set<Integer> expected = ImmutableSet.of(1, 5);
        assertEquals(actual, expected);

        actual = mapColumnToNode(ImmutableSet.of(1), orcTypes);
        expected = ImmutableSet.of(2);
        assertEquals(actual, expected);
    }

    @Test
    public void testMapColumnToNodeEmpty()
    {
        Set<Integer> actual = mapColumnToNode(ImmutableSet.of(), ImmutableList.of());
        assertTrue(actual.isEmpty());
    }

    @Test
    public void testMapColumnToNodeIgnoreMissingColumns()
    {
        List<String> columnNames = ImmutableList.of("f1");
        List<Type> columnTypes = ImmutableList.of(VARCHAR);
        List<OrcType> orcTypes = OrcType.createOrcRowType(0, columnNames, columnTypes);
        Set<Integer> actual = mapColumnToNode(ImmutableSet.of(0, 100, 200), orcTypes);
        Set<Integer> expected = ImmutableSet.of(1);
        assertEquals(actual, expected);
    }
}
