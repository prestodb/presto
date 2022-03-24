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
package com.facebook.presto.common.type;

import com.facebook.presto.common.QualifiedObjectName;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.TypeUtils.containsDistinctType;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestTypeUtils
{
    @Test
    public void testContainsDistinctType()
    {
        QualifiedObjectName distinctTypeName = QualifiedObjectName.valueOf("test.dt.int00");
        DistinctTypeInfo distinctTypeInfo = new DistinctTypeInfo(distinctTypeName, INTEGER.getTypeSignature(), Optional.empty(), false);
        DistinctType distinctType = new DistinctType(distinctTypeInfo, INTEGER, null);

        // check primitives
        assertFalse(containsDistinctType(ImmutableList.of(INTEGER, VARCHAR)));

        // check top level
        assertTrue(containsDistinctType(ImmutableList.of(distinctType)));

        // check first nesting level
        assertFalse(containsDistinctType(ImmutableList.of(RowType.anonymous(ImmutableList.of(INTEGER, VARCHAR)))));
        assertFalse(containsDistinctType(ImmutableList.of(new ArrayType(INTEGER))));
        assertTrue(containsDistinctType(ImmutableList.of(RowType.anonymous(ImmutableList.of(INTEGER, distinctType)))));
        assertTrue(containsDistinctType(ImmutableList.of(new ArrayType(distinctType))));

        // check deep nesting
        assertFalse(containsDistinctType(ImmutableList.of(new ArrayType(new ArrayType(INTEGER)))));
        assertFalse(containsDistinctType(ImmutableList.of(new ArrayType(RowType.anonymous(ImmutableList.of(INTEGER, VARCHAR))))));
        assertTrue(containsDistinctType(ImmutableList.of(new ArrayType(new ArrayType(distinctType)))));
        assertTrue(containsDistinctType(ImmutableList.of(new ArrayType(RowType.anonymous(ImmutableList.of(INTEGER, distinctType))))));
    }
}
