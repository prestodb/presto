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
package com.facebook.presto.serde;

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.TypeRegistry;
import io.airlift.slice.DynamicSliceOutput;
import org.testng.annotations.Test;

import static com.facebook.presto.serde.TypeSerde.readType;
import static com.facebook.presto.serde.TypeSerde.writeInfo;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static org.testng.Assert.assertEquals;

public class TestTypeSerde
{
    @Test
    public void testRoundTrip()
    {
        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1024);
        writeInfo(sliceOutput, BOOLEAN);
        Type actualType = readType(new TypeRegistry(), sliceOutput.slice().getInput());
        assertEquals(actualType, BOOLEAN);
    }
}
