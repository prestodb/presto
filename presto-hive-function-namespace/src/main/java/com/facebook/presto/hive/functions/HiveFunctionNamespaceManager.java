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

package com.facebook.presto.hive.functions;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.common.function.SqlFunctionResult;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.UserDefinedType;
import com.facebook.presto.hive.functions.aggregation.HiveAggregationFunction;
import com.facebook.presto.hive.functions.scalar.HiveScalarFunction;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.function.AggregationFunctionImplementation;
import com.facebook.presto.spi.function.AlterRoutineCharacteristics;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.FunctionNamespaceManager;
import com.facebook.presto.spi.function.FunctionNamespaceTransactionHandle;
import com.facebook.presto.spi.function.ScalarFunctionImplementation;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlFunctionVisibility;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver2;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;

import javax.inject.Inject;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import static com.facebook.presto.hive.functions.FunctionRegistry.getCurrentFunctionNames;
import static com.facebook.presto.hive.functions.HiveFunctionErrorCode.functionNotFound;
import static com.facebook.presto.hive.functions.HiveFunctionErrorCode.unsupportedFunctionType;
import static com.facebook.presto.hive.functions.aggregation.HiveAggregationFunction.createHiveAggregateFunction;
import static com.facebook.presto.hive.functions.scalar.HiveScalarFunction.createHiveScalarFunction;
import static com.facebook.presto.spi.function.FunctionKind.SCALAR;
import static com.facebook.presto.spi.function.SqlFunctionVisibility.PUBLIC;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class HiveFunctionNamespaceManager
        implements FunctionNamespaceManager<HiveFunction>
{
    public static final String ID = "hive-functions";

    private final String catalogName;
    private final ClassLoader classLoader;
    private final LoadingCache<FunctionKey, HiveFunction> functions;
    private final HiveFunctionRegistry hiveFunctionRegistry;
    private final TypeManager typeManager;

    @Inject
    public HiveFunctionNamespaceManager(
            @ForHiveFunction String catalogName,
            @ForHiveFunction ClassLoader classLoader,
            HiveFunctionRegistry hiveFunctionRegistry,
            TypeManager typeManager)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.hiveFunctionRegistry = requireNonNull(hiveFunctionRegistry, "hiveFunctionRegistry is null");
        this.classLoader = requireNonNull(classLoader, "classLoader is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.functions = CacheBuilder.newBuilder().maximumSize(1000)
                .build(CacheLoader.from(this::initializeFunction));
    }

    @Override
    public void setBlockEncodingSerde(BlockEncodingSerde blockEncodingSerde)
    {
        return;
    }

    /**
     * this function namespace manager does not support transaction.
     */
    public FunctionNamespaceTransactionHandle beginTransaction()
    {
        return new EmptyTransactionHandle();
    }

    public void commit(FunctionNamespaceTransactionHandle transactionHandle)
    {
    }

    public void abort(FunctionNamespaceTransactionHandle transactionHandle)
    {
    }

    public void createFunction(SqlInvokedFunction function, boolean replace)
    {
        throw new IllegalStateException(format("Cannot create function in hive function namespace: %s", function.getSignature().getName()));
    }

    public void alterFunction(QualifiedObjectName functionName, Optional<List<TypeSignature>> parameterTypes, AlterRoutineCharacteristics alterRoutineCharacteristics)
    {
        throw new IllegalStateException(format("Cannot alter function in hive function namespace: %s", functionName));
    }

    public void dropFunction(QualifiedObjectName functionName, Optional<List<TypeSignature>> parameterTypes, boolean exists)
    {
        throw new IllegalStateException(format("Cannot drop function in hive function namespace: %s", functionName));
    }

    @Override
    public Collection<HiveFunction> listFunctions(Optional<String> likePattern, Optional<String> escape)
    {
        return getCurrentFunctionNames().stream().map(functionName -> createDummyHiveFunction(functionName)).collect(toImmutableList());
    }

    public Collection<HiveFunction> getFunctions(Optional<? extends FunctionNamespaceTransactionHandle> transactionHandle, QualifiedObjectName functionName)
    {
        throw new IllegalStateException("Get function is not supported");
    }

    public FunctionHandle getFunctionHandle(Optional<? extends FunctionNamespaceTransactionHandle> transactionHandle, Signature signature)
    {
        throw new IllegalStateException("Get function handle is not supported");
    }

    public FunctionHandle resolveFunction(Optional<? extends FunctionNamespaceTransactionHandle> transactionHandle, QualifiedObjectName functionName, List<TypeSignature> parameterTypes)
    {
        return new HiveFunctionHandle(initializeFunction(FunctionKey.of(functionName, parameterTypes)).getSignature());
    }

    public FunctionMetadata getFunctionMetadata(FunctionHandle functionHandle)
    {
        checkArgument(functionHandle instanceof HiveFunctionHandle);
        HiveFunction function = functions.getUnchecked(FunctionKey.from((HiveFunctionHandle) functionHandle));
        return function.getFunctionMetadata();
    }

    public ScalarFunctionImplementation getScalarFunctionImplementation(FunctionHandle functionHandle)
    {
        checkArgument(functionHandle instanceof HiveFunctionHandle);
        HiveFunction function = functions.getUnchecked(FunctionKey.from((HiveFunctionHandle) functionHandle));
        checkState(function instanceof HiveScalarFunction);
        return ((HiveScalarFunction) function).getJavaScalarFunctionImplementation();
    }

    @Override
    public CompletableFuture<SqlFunctionResult> executeFunction(String source, FunctionHandle functionHandle, Page input, List<Integer> channels, TypeManager typeManager)
    {
        throw new IllegalStateException("Execute function is not supported");
    }

    @Override
    public void addUserDefinedType(UserDefinedType userDefinedType)
    {
        throw new IllegalStateException("User defined type is not supported");
    }

    @Override
    public Optional<UserDefinedType> getUserDefinedType(QualifiedObjectName typeName)
    {
        throw new IllegalStateException("User defined type is not supported");
    }

    @Override
    public boolean canResolveFunction()
    {
        return true;
    }

    private HiveFunction initializeFunction(FunctionKey key)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            QualifiedObjectName name = key.getName();
            try {
                Class<?> functionClass = hiveFunctionRegistry.getClass(name);
                if (anyAssignableFrom(functionClass, GenericUDF.class, UDF.class)) {
                    return createHiveScalarFunction(functionClass, name, key.getArgumentTypes(), typeManager);
                }
                else if (anyAssignableFrom(GenericUDAFResolver2.class, GenericUDAFResolver.class, UDAF.class)) {
                    return createHiveAggregateFunction(functionClass, name, key.getArgumentTypes(), typeManager);
                }
                else {
                    throw unsupportedFunctionType(functionClass);
                }
            }
            catch (ClassNotFoundException e) {
                e.printStackTrace();
                throw functionNotFound(name.toString());
            }
        }
    }

    private HiveFunction createDummyHiveFunction(String functionName)
    {
        QualifiedObjectName hiveFunctionName = QualifiedObjectName.valueOf(catalogName, "default", functionName);
        Signature signature = new Signature(hiveFunctionName, SCALAR, TypeSignature.parseTypeSignature("T"));
        return new DummyHiveFunction(signature);
    }

    private static boolean anyAssignableFrom(Class<?> cls, Class<?>... supers)
    {
        return Stream.of(supers).anyMatch(s -> s.isAssignableFrom(cls));
    }

    @Override
    public AggregationFunctionImplementation getAggregateFunctionImplementation(FunctionHandle functionHandle)
    {
        checkArgument(functionHandle instanceof HiveFunctionHandle);
        HiveFunction function = functions.getUnchecked(FunctionKey.from(((HiveFunctionHandle) functionHandle)));
        verify(function instanceof HiveAggregationFunction);
        return ((HiveAggregationFunction) function).getImplementation();
    }

    private static class DummyHiveFunction
            extends HiveFunction
    {
        public DummyHiveFunction(Signature signature)
        {
            super(signature.getName(), signature, false, true, true, "");
        }

        @Override
        public FunctionMetadata getFunctionMetadata()
        {
            throw new IllegalStateException("Get function metadata is not supported");
        }

        @Override
        public SqlFunctionVisibility getVisibility()
        {
            return PUBLIC;
        }
    }

    private static class EmptyTransactionHandle
            implements FunctionNamespaceTransactionHandle
    {
    }

    private static class FunctionKey
    {
        private final QualifiedObjectName name;
        private final List<TypeSignature> argumentTypes;

        public static FunctionKey from(HiveFunctionHandle handle)
        {
            Signature signature = handle.getSignature();
            return FunctionKey.of(signature.getName(), signature.getArgumentTypes());
        }

        private static FunctionKey of(QualifiedObjectName name, List<TypeSignature> argumentTypes)
        {
            return new FunctionKey(name, argumentTypes);
        }

        private FunctionKey(QualifiedObjectName name, List<TypeSignature> argumentTypes)
        {
            this.name = requireNonNull(name, "name is null");
            this.argumentTypes = ImmutableList.copyOf(requireNonNull(argumentTypes, "argumentTypes is null"));
        }

        public QualifiedObjectName getName()
        {
            return name;
        }

        public List<TypeSignature> getArgumentTypes()
        {
            return argumentTypes;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            FunctionKey that = (FunctionKey) o;
            return Objects.equals(name, that.name) &&
                    Objects.equals(argumentTypes, that.argumentTypes);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(name, argumentTypes);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("name", name)
                    .add("arguments", argumentTypes)
                    .toString();
        }
    }
}
