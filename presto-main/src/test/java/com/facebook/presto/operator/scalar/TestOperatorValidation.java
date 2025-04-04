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

import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.metadata.FunctionListBuilder;
import com.facebook.presto.spi.function.ScalarOperator;
import com.facebook.presto.spi.function.SqlType;
import org.testng.annotations.Test;

import static com.facebook.presto.common.function.OperatorType.ADD;

@SuppressWarnings("UtilityClassWithoutPrivateConstructor")
public class TestOperatorValidation
{
    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "ADD operator must have exactly 2 argument.*")
    public void testInvalidArgumentCount()
    {
        extractScalars(InvalidArgumentCount.class);
    }

    public static final class InvalidArgumentCount
    {
        @ScalarOperator(ADD)
        @SqlType(StandardTypes.BIGINT)
        public static long add(@SqlType(StandardTypes.BIGINT) long value)
        {
            return 0;
        }
    }

    private static void extractScalars(Class<?> clazz)
    {
        new FunctionListBuilder().scalars(clazz);
    }
}
