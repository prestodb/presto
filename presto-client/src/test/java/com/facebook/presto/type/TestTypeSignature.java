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

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.type.TypeSignature.parseTypeSignature;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestTypeSignature
{
    @Test
    public void test()
            throws Exception
    {
        assertSignature("bigint", ImmutableList.<String>of());
        assertSignature("boolean", ImmutableList.<String>of());
        assertSignature("varchar", ImmutableList.<String>of());
        assertSignature("array", ImmutableList.of("bigint"));
        assertSignature("array", ImmutableList.of("array<bigint>"));
        assertSignature("map", ImmutableList.of("bigint", "bigint"));
        assertSignature("map", ImmutableList.of("bigint", "array<bigint>"));
        assertSignature("map", ImmutableList.of("bigint", "map<bigint,map<varchar,bigint>>"));
        try {
            parseTypeSignature("blah<>");
            fail("Type signatures with zero parameters should fail to parse");
        }
        catch (RuntimeException e) {
            // Expected
        }
    }

    private static void assertSignature(String base, List<String> parameters)
    {
        List<String> lowerCaseTypeNames = FluentIterable.from(parameters).transform(new Function<String, String>() {
            @Override
            public String apply(String input)
            {
                return input.toLowerCase();
            }
        }).toList();

        String typeName = base.toLowerCase();
        if (!parameters.isEmpty()) {
            typeName += "<" + Joiner.on(",").join(lowerCaseTypeNames) + ">";
        }
        TypeSignature signature = parseTypeSignature(typeName);
        assertEquals(signature.getBase(), base);
        assertEquals(signature.getParameters().size(), parameters.size());
        for (int i = 0; i < signature.getParameters().size(); i++) {
            assertEquals(signature.getParameters().get(i).toString(), parameters.get(i));
        }
        assertEquals(typeName, signature.toString());
    }
}
