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
package io.prestosql.operator.scalar;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.prestosql.metadata.BoundVariables;
import io.prestosql.metadata.FunctionKind;
import io.prestosql.metadata.FunctionRegistry;
import io.prestosql.metadata.Signature;
import io.prestosql.metadata.SqlScalarFunction;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;

import java.lang.invoke.MethodHandle;

import static io.prestosql.metadata.Signature.typeVariable;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.util.Reflection.methodHandle;

public class ElementToArrayConcatFunction
        extends SqlScalarFunction
{
    public static final ElementToArrayConcatFunction ELEMENT_TO_ARRAY_CONCAT_FUNCTION = new ElementToArrayConcatFunction();
    private static final String FUNCTION_NAME = "concat";

    private static final MethodHandle METHOD_HANDLE_BOOLEAN = methodHandle(ArrayConcatUtils.class, "prependElement", Type.class, boolean.class, Block.class);
    private static final MethodHandle METHOD_HANDLE_LONG = methodHandle(ArrayConcatUtils.class, "prependElement", Type.class, long.class, Block.class);
    private static final MethodHandle METHOD_HANDLE_DOUBLE = methodHandle(ArrayConcatUtils.class, "prependElement", Type.class, double.class, Block.class);
    private static final MethodHandle METHOD_HANDLE_SLICE = methodHandle(ArrayConcatUtils.class, "prependElement", Type.class, Slice.class, Block.class);
    private static final MethodHandle METHOD_HANDLE_OBJECT = methodHandle(ArrayConcatUtils.class, "prependElement", Type.class, Object.class, Block.class);

    public ElementToArrayConcatFunction()
    {
        super(new Signature(
                FUNCTION_NAME,
                FunctionKind.SCALAR,
                ImmutableList.of(typeVariable("E")),
                ImmutableList.of(),
                parseTypeSignature("array(E)"),
                ImmutableList.of(parseTypeSignature("E"), parseTypeSignature("array(E)")),
                false));
    }

    @Override
    public boolean isHidden()
    {
        return false;
    }

    @Override
    public boolean isDeterministic()
    {
        return true;
    }

    @Override
    public String getDescription()
    {
        return "Concatenates an element to an array";
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type type = boundVariables.getTypeVariable("E");
        MethodHandle methodHandle;
        if (type.getJavaType() == boolean.class) {
            methodHandle = METHOD_HANDLE_BOOLEAN;
        }
        else if (type.getJavaType() == long.class) {
            methodHandle = METHOD_HANDLE_LONG;
        }
        else if (type.getJavaType() == double.class) {
            methodHandle = METHOD_HANDLE_DOUBLE;
        }
        else if (type.getJavaType() == Slice.class) {
            methodHandle = METHOD_HANDLE_SLICE;
        }
        else {
            methodHandle = METHOD_HANDLE_OBJECT;
        }
        methodHandle = methodHandle.bindTo(type);

        return new ScalarFunctionImplementation(
                false,
                ImmutableList.of(
                        valueTypeArgumentProperty(RETURN_NULL_ON_NULL),
                        valueTypeArgumentProperty(RETURN_NULL_ON_NULL)),
                methodHandle,
                isDeterministic());
    }
}
