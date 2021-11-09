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
import com.facebook.presto.hive.functions.scalar.HiveScalarFunction;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.function.AlterRoutineCharacteristics;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.FunctionNamespaceManager;
import com.facebook.presto.spi.function.FunctionNamespaceTransactionHandle;
import com.facebook.presto.spi.function.ScalarFunctionImplementation;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.facebook.presto.spi.type.TypeManagerAware;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;

import javax.inject.Inject;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static com.facebook.presto.hive.functions.HiveFunctionErrorCode.functionNotFound;
import static com.facebook.presto.hive.functions.HiveFunctionErrorCode.unsupportedFunctionType;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class HiveFunctionNamespaceManager
        implements FunctionNamespaceManager<HiveFunction>, TypeManagerAware
{
    private static final long FUNCTION_NAME_EXPIRE_MILLS = 7_200_000;
    public static final String ID = "hive-functions";

    private final ClassLoader classLoader;
    private final LoadingCache<FunctionKey, HiveFunction> functions;
    private final LoadingCache<QualifiedObjectName, Boolean> functionNames;
    private final HiveFunctionRegistry hiveFunctionRegistry;
    private TypeManager typeManager;

    @Inject
    public HiveFunctionNamespaceManager(
            @ForHiveFunction ClassLoader classLoader,
            HiveFunctionRegistry hiveFunctionRegistry)
    {
        this.hiveFunctionRegistry = hiveFunctionRegistry;
        this.classLoader = classLoader;
        this.functions = CacheBuilder.newBuilder()
                .build(CacheLoader.from(this::createFunction));
        this.functionNames = CacheBuilder.newBuilder()
                .expireAfterWrite(FUNCTION_NAME_EXPIRE_MILLS, TimeUnit.MILLISECONDS)
                .build(CacheLoader.from(name -> {
                    try {
                        hiveFunctionRegistry.getClass(name);
                    }
                    catch (ClassNotFoundException e) {
                        return false;
                    }
                    return true;
                }));
    }

    @Override
    public void setBlockEncodingSerde(BlockEncodingSerde blockEncodingSerde)
    {
        return;
    }

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
        throw new PrestoException(GENERIC_USER_ERROR, format("Cannot create function in hive function namespace: %s", function.getSignature().getName()));
    }

    public void alterFunction(QualifiedObjectName functionName, Optional<List<TypeSignature>> parameterTypes, AlterRoutineCharacteristics alterRoutineCharacteristics)
    {
        throw new PrestoException(GENERIC_USER_ERROR, format("Cannot alter function in hive function namespace: %s", functionName));
    }

    public void dropFunction(QualifiedObjectName functionName, Optional<List<TypeSignature>> parameterTypes, boolean exists)
    {
        throw new PrestoException(GENERIC_USER_ERROR, format("Cannot drop function in hive function namespace: %s", functionName));
    }

    @Override
    public Collection<HiveFunction> listFunctions(Optional<String> likePattern, Optional<String> escape)
    {
        // TODO Implement list functions
        return Collections.emptyList();
    }

    public Collection<HiveFunction> getFunctions(Optional<? extends FunctionNamespaceTransactionHandle> transactionHandle, QualifiedObjectName functionName)
    {
        // TODO A better exception
        throw new PrestoException(StandardErrorCode.GENERIC_INTERNAL_ERROR, "Not supported");
    }

    public FunctionHandle getFunctionHandle(Optional<? extends FunctionNamespaceTransactionHandle> transactionHandle, Signature signature)
    {
        return new HiveFunctionHandle(requireNonNull(signature, "signature is null"));
    }

    public FunctionHandle getFunctionHandle(Optional<? extends FunctionNamespaceTransactionHandle> transactionHandle, QualifiedObjectName functionName, List<TypeSignature> parameterTypes)
    {
        return new HiveFunctionHandle(createFunction(FunctionKey.of(functionName, parameterTypes)).getSignature());
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
        verify(function instanceof HiveScalarFunction);
        return ((HiveScalarFunction) function).getScalarFunctionImplementation();
    }

    @Override
    public CompletableFuture<SqlFunctionResult> executeFunction(String source, FunctionHandle functionHandle, Page input, List<Integer> channels, TypeManager typeManager)
    {
        return null;
    }

    @Override
    public void addUserDefinedType(UserDefinedType userDefinedType)
    {
        return;
    }

    @Override
    public Optional<UserDefinedType> getUserDefinedType(QualifiedObjectName typeName)
    {
        return Optional.empty();
    }

    @Override
    public void setTypeManager(TypeManager typeManager)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public boolean resolveFunction()
    {
        return true;
    }

    private HiveFunction createFunction(FunctionKey key)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            QualifiedObjectName name = key.getName();
            try {
                Class<?> functionClass = hiveFunctionRegistry.getClass(name);
                if (anyAssignableFrom(functionClass, GenericUDF.class, UDF.class)) {
                    return HiveScalarFunction.create(functionClass, name, key.getArgumentTypes(), typeManager);
                }
                else {
                    throw unsupportedFunctionType(functionClass);
                }
            }
            catch (ClassNotFoundException e) {
                throw functionNotFound(name.toString());
            }
        }
    }

    private static boolean anyAssignableFrom(Class<?> cls, Class<?>... supers)
    {
        return Stream.of(supers).anyMatch(s -> s.isAssignableFrom(cls));
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
            final Signature signature = handle.getSignature();
            return FunctionKey.of(signature.getName(), signature.getArgumentTypes());
        }

        private static FunctionKey of(QualifiedObjectName name, List<TypeSignature> argumentTypes)
        {
            return new FunctionKey(name, argumentTypes);
        }

        private FunctionKey(QualifiedObjectName name, List<TypeSignature> argumentTypes)
        {
            this.name = name;
            this.argumentTypes = ImmutableList.copyOf(argumentTypes);
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
