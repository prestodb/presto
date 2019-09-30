package com.facebook.presto.translator.registry;

import com.facebook.presto.spi.function.FunctionMetadata;
import com.google.common.collect.ImmutableMap;

import java.lang.invoke.MethodHandle;
import java.util.Arrays;

import static com.facebook.presto.translator.registry.ScalarFromAnnotationsParser.parseFunctionDefinitions;

public class FunctionRegistry
{
    private final ImmutableMap<FunctionMetadata, MethodHandle> translators;

    public FunctionRegistry(Class<?>... classes)
    {
        ImmutableMap.Builder<FunctionMetadata, MethodHandle> functions = ImmutableMap.builder();
        Arrays.stream(classes).forEach(functionDefinition -> {
            functions.putAll(parseFunctionDefinitions(functionDefinition));
        });

        this.translators = functions.build();
    }

    public ImmutableMap<FunctionMetadata, MethodHandle> getTranslators()
    {
        return translators;
    }

    public MethodHandle getTranslatorForFunction(FunctionMetadata function)
    {
        return translators.get(function);
    }
}
