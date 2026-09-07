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
package com.facebook.presto.type;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.facebook.presto.type.ChangeKindEnumType.CHANGE_KIND;
import static com.facebook.presto.type.ChangeKindEnumType.USER_DEFINED_TYPE;
import static org.testng.Assert.assertEquals;

public class TestChangeKindEnumType
{
    @Test
    public void testChangeKinds()
    {
        assertEquals(
                CHANGE_KIND.getEnumMap(),
                ImmutableMap.of(
                        "INSERT", "INSERT",
                        "DELETE", "DELETE",
                        "UPDATE_BEFORE", "UPDATE_BEFORE",
                        "UPDATE_AFTER", "UPDATE_AFTER"));
    }

    @Test
    public void testRegisteredTypeResolvesByName()
    {
        FunctionAndTypeManager functionAndTypeManager = createTestFunctionAndTypeManager();
        functionAndTypeManager.addUserDefinedType(USER_DEFINED_TYPE);

        Type type = functionAndTypeManager.getType(new TypeSignature(USER_DEFINED_TYPE));
        assertEquals(type.getDisplayName(), "presto.default.change_kind");
        assertEquals(type.getTypeSignature(), new TypeSignature(USER_DEFINED_TYPE));
    }
}
