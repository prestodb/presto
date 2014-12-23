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
package com.facebook.presto.spi.type;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.DynamicSliceOutput;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.TypeSerde.readType;
import static com.facebook.presto.spi.type.TypeSerde.writeType;
import static org.testng.Assert.assertEquals;

public class TestTypeSerde
{
    @Test
    public void testRoundTrip()
    {
        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1024);
        writeType(sliceOutput, BOOLEAN);
        Type actualType = readType(new TestingTypeRegistry(), sliceOutput.slice().getInput());
        assertEquals(actualType, BOOLEAN);
    }

    private static class TestingTypeRegistry
            implements TypeManager
    {
        @Override
        public Type getType(TypeSignature signature)
        {
            for (Type type : getTypes()) {
                if (signature.equals(type.getTypeSignature())) {
                    return type;
                }
            }
            return null;
        }

        @Override
        public Type getParameterizedType(String baseTypeName, List<TypeSignature> typeParameters)
        {
            return getType(new TypeSignature(baseTypeName, typeParameters));
        }

        @Override
        public List<Type> getTypes()
        {
            return ImmutableList.<Type>of(BOOLEAN);
        }
    }
}
