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
package com.facebook.presto.tests;

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation;
import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlFunctionVisibility;
import com.google.common.collect.ImmutableList;

import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.metadata.BuiltInTypeAndFunctionNamespaceManager.DEFAULT_NAMESPACE;
import static com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static com.facebook.presto.spi.function.Signature.typeVariable;
import static com.facebook.presto.spi.function.SqlFunctionVisibility.HIDDEN;
import static com.facebook.presto.util.Reflection.constructorMethodHandle;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Collections.nCopies;

public class StatefulSleepingSum
        extends SqlScalarFunction
{
    public static final StatefulSleepingSum STATEFUL_SLEEPING_SUM = new StatefulSleepingSum();

    private StatefulSleepingSum()
    {
        super(new Signature(
                QualifiedObjectName.valueOf(DEFAULT_NAMESPACE, "stateful_sleeping_sum"),
                FunctionKind.SCALAR,
                ImmutableList.of(typeVariable("bigint")),
                ImmutableList.of(),
                parseTypeSignature("bigint"),
                ImmutableList.of(parseTypeSignature("double"), parseTypeSignature("bigint"), parseTypeSignature("bigint"), parseTypeSignature("bigint")),
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
        return true;
    }

    @Override
    public String getDescription()
    {
        return "testing not thread safe function";
    }

    @Override
    public BuiltInScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
    {
        int args = 4;
        return new BuiltInScalarFunctionImplementation(
                false,
                nCopies(args, valueTypeArgumentProperty(RETURN_NULL_ON_NULL)),
                methodHandle(StatefulSleepingSum.class, "statefulSleepingSum", State.class, double.class, long.class, long.class, long.class),
                Optional.of(constructorMethodHandle(State.class)));
    }

    public static long statefulSleepingSum(State state, double sleepProbability, long sleepDurationMillis, long a, long b)
    {
        int currentThreads = state.currentThreads.incrementAndGet();
        try {
            checkState(currentThreads == 1, "%s threads concurrently executing a stateful function", currentThreads);
            if (ThreadLocalRandom.current().nextDouble() < sleepProbability) {
                Thread.sleep(sleepDurationMillis);
            }
            return a + b;
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted", e);
        }
        finally {
            state.currentThreads.decrementAndGet();
        }
    }

    public static class State
    {
        private final AtomicInteger currentThreads = new AtomicInteger();
    }
}
