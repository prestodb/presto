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
package com.facebook.presto.sql.relational;

import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.type.Type;

import java.util.Arrays;
import java.util.List;

public final class Expressions
{
    private Expressions()
    {
    }

    public static ConstantExpression constant(Object value, Type type)
    {
        return new ConstantExpression(value, type);
    }

    public static ConstantExpression constantNull(Type type)
    {
        return new ConstantExpression(null, type);
    }

    public static CallExpression call(Signature signature, RowExpression... arguments)
    {
        return new CallExpression(signature, Arrays.asList(arguments));
    }

    public static CallExpression call(Signature signature, List<RowExpression> arguments)
    {
        return new CallExpression(signature, arguments);
    }

    public static InputReferenceExpression field(int field, Type type)
    {
        return new InputReferenceExpression(field, type);
    }
}
