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
package com.facebook.presto.metadata;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.BuiltInType;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.Parameter;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlFunction;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.google.common.util.concurrent.UncheckedExecutionException;

import javax.inject.Provider;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.function.FunctionImplementationType.CPP;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class BuiltInNativeFunctionNamespaceManager
        extends BuiltInSpecialFunctionNamespaceManager
{
    private static final Logger log = Logger.get(BuiltInNativeFunctionNamespaceManager.class);

    private final Provider<NativeFunctionRegistryTool> nativeFunctionRegistryToolProvider;

    public BuiltInNativeFunctionNamespaceManager(FunctionAndTypeManager functionAndTypeManager, Provider<NativeFunctionRegistryTool> nativeFunctionRegistryToolProvider)
    {
        super(functionAndTypeManager);
        this.nativeFunctionRegistryToolProvider = requireNonNull(nativeFunctionRegistryToolProvider, "nativeFunctionRegistryToolProvider is null");
    }

    public synchronized void registerNativeFunctions()
    {
        // only register functions once
        if (!this.functions.list().isEmpty()) {
            return;
        }

        List<SqlFunction> allNativeFunctions = nativeFunctionRegistryToolProvider
                .get()
                .getNativeFunctionSignatureMap()
                .getUDFSignatureMap()
                .entrySet()
                .stream()
                .flatMap(entry -> entry.getValue().stream()
                        .map(metaInfo -> NativeSidecarFunctionUtil.createSqlInvokedFunction(entry.getKey(), metaInfo)))
                .collect(Collectors.toList());
        this.functions = new FunctionMap(this.functions, allNativeFunctions);
    }

    @Override
    public FunctionHandle getFunctionHandle(Signature signature)
    {
        return new BuiltInFunctionHandle(signature, BuiltInType.NATIVE);
    }

    @Override
    public FunctionMetadata getFunctionMetadata(FunctionHandle functionHandle)
    {
        checkArgument(functionHandle instanceof BuiltInFunctionHandle, "Expect BuiltInFunctionHandle");
        Signature signature = ((BuiltInFunctionHandle) functionHandle).getSignature();
        SpecializedFunctionKey functionKey;
        try {
            functionKey = specializedFunctionKeyCache.getUnchecked(signature);
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), PrestoException.class);
            throw e;
        }
        SqlFunction function = functionKey.getFunction();
        checkArgument(function instanceof SqlInvokedFunction, "BuiltInPluginFunctionNamespaceManager only support SqlInvokedFunctions");
        SqlInvokedFunction sqlFunction = (SqlInvokedFunction) function;
        List<String> argumentNames = sqlFunction.getParameters().stream().map(Parameter::getName).collect(toImmutableList());
        return new FunctionMetadata(
                signature.getName(),
                signature.getArgumentTypes(),
                argumentNames,
                signature.getReturnType(),
                signature.getKind(),
                sqlFunction.getRoutineCharacteristics().getLanguage(),
                CPP,
                function.isDeterministic(),
                function.isCalledOnNullInput(),
                sqlFunction.getVersion(),
                sqlFunction.getComplexTypeFunctionDescriptor());
    }

    @Override
    protected synchronized void checkForNamingConflicts(Collection<? extends SqlFunction> functions)
    {
    }
}
