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
package com.facebook.presto.sql.gen;

import com.facebook.presto.bytecode.Binding;
import com.facebook.presto.bytecode.CallSiteBinder;
import com.facebook.presto.bytecode.StaticTypeBytecodeExpression;
import com.facebook.presto.common.type.Type;

import java.lang.reflect.Method;

import static com.facebook.presto.sql.gen.Bootstrap.BOOTSTRAP_METHOD;
import static java.util.Objects.requireNonNull;

public class SqlTypeBytecodeExpression
        extends StaticTypeBytecodeExpression
{
    public static SqlTypeBytecodeExpression constantType(CallSiteBinder callSiteBinder, Type type)
    {
        requireNonNull(callSiteBinder, "callSiteBinder is null");
        requireNonNull(type, "type is null");

        Binding binding = callSiteBinder.bind(type, Type.class);
        return new SqlTypeBytecodeExpression(type, binding, BOOTSTRAP_METHOD);
    }

    private static String generateName(Type type)
    {
        String name = type.getTypeSignature().toString();
        if (name.length() > 20) {
            // Use type base to reduce the identifier size in generated code
            name = type.getTypeSignature().getBase();
        }
        return name.replaceAll("\\W+", "_");
    }

    private SqlTypeBytecodeExpression(Type type, Binding binding, Method bootstrapMethod)
    {
        super(binding, bootstrapMethod, Type.class, generateName(type), type.getJavaType());
    }
}
