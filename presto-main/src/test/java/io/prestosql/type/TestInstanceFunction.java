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
package io.prestosql.type;

import io.prestosql.operator.scalar.AbstractTestFunctions;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.prestosql.spi.type.BigintType.BIGINT;

public class TestInstanceFunction
        extends AbstractTestFunctions
{
    @BeforeClass
    public void setUp()
    {
        registerParametricScalar(PrecomputedFunction.class);
    }

    @Test
    public void test()
    {
        assertFunction("precomputed()", BIGINT, 42L);
    }

    @ScalarFunction("precomputed")
    public static final class PrecomputedFunction
    {
        private final int value = 42;

        @SqlType(StandardTypes.BIGINT)
        public long precomputed()
        {
            return value;
        }
    }
}
