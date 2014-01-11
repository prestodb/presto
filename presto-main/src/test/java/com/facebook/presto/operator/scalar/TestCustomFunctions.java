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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.metadata.FunctionFactory;
import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.FunctionRegistry.FunctionListBuilder;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.operator.scalar.FunctionAssertions.assertFunction;
import static com.facebook.presto.operator.scalar.FunctionAssertions.getMetadataManager;

public class TestCustomFunctions
{
    @BeforeClass
    public void setupClass()
    {
        getMetadataManager().addFunctions(new FunctionFactory()
        {
            @Override
            public List<FunctionInfo> listFunctions()
            {
                return new FunctionListBuilder("custom")
                        .scalar(CustomAdd.class)
                        .build();
            }
        });
    }

    @Test
    public void testCustomAdd()
    {
        assertFunction("custom_add(123,456)", 579L);
    }

    public static class CustomAdd
    {
        @ScalarFunction
        public static long customAdd(long lhs, long rhs)
        {
            return lhs + rhs;
        }
    }
}
