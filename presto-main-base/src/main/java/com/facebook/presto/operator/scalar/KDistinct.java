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

import com.facebook.presto.annotation.UsedByGeneratedCode;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlFunctionVisibility;
import com.facebook.presto.sql.gen.lambda.BinaryFunctionInterface;
import com.google.common.collect.ImmutableList;
import it.unimi.dsi.fastutil.longs.LongArraySet;

import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.metadata.BuiltInTypeAndFunctionNamespaceManager.DEFAULT_NAMESPACE;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.ArgumentProperty.functionTypeArgumentProperty;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.ArgumentProperty.valueTypeArgumentProperty;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.NullConvention.RETURN_NULL_ON_NULL;
import static com.facebook.presto.spi.function.SqlFunctionVisibility.HIDDEN;
import static com.facebook.presto.util.Reflection.methodHandle;

public final class KDistinct
        extends SqlScalarFunction
{
    public static final KDistinct K_DISTINCT = new KDistinct();
    private static final MethodHandle STATE_FACTORY = methodHandle(KDistinct.class, "createState");

    private KDistinct()
    {
        super(new Signature(
                QualifiedObjectName.valueOf(DEFAULT_NAMESPACE, "k_distinct"),
                FunctionKind.SCALAR,
                ImmutableList.of(),
                ImmutableList.of(),
                BOOLEAN.getTypeSignature(),
                ImmutableList.of(BIGINT.getTypeSignature(), BIGINT.getTypeSignature()),
                false));
    }

    @Override
    public SqlFunctionVisibility getVisibility()
    {
        return HIDDEN;
    }

    @Override
    public boolean isDeterministic()
    {
        return false;
    }

    @Override
    public String getDescription()
    {
        return "Returns true for the first K distinct hash values updating the state if it's not already in";
    }

    @Override
    public BuiltInScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
    {
        return new BuiltInScalarFunctionImplementation(
                false,
                ImmutableList.of(
                        valueTypeArgumentProperty(RETURN_NULL_ON_NULL),
                        functionTypeArgumentProperty(BinaryFunctionInterface.class)),
                methodHandle(KDistinct.class, "addKey", Object.class, long.class, long.class),
                Optional.of(methodHandle(KDistinct.class, "createState")));
    }

    @UsedByGeneratedCode
    public static boolean addKey(Object state, long hash, long k)
    {
        State currentState = (State) state;
        if (currentState.longArraySet != null) {
            if (currentState.longArraySet.size() == k) {
                return false;
            }
        }
        else {
            currentState.longArraySet = new LongArraySet(new long[(int) k]);
        }

        return currentState.longArraySet.add(hash);
    }

    @UsedByGeneratedCode
    public static Object createState()
    {
        return new State();
    }

    private static class State
    {
        public LongArraySet longArraySet;
    }
}
