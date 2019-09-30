package com.facebook.presto.translator.registry;

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.collect.ImmutableSet;

import java.util.Arrays;
import java.util.Set;

import static com.facebook.presto.translator.registry.ScalarFromAnnotationsParser.removeTypeParameters;

public class ConstantTypeRegistry
{
    private final Set<TypeSignature> constantTypes;

    public ConstantTypeRegistry(Type... types)
    {
        ImmutableSet.Builder<TypeSignature> constantTypes = ImmutableSet.builder();
        Arrays.stream(types).forEach(type -> {
            constantTypes.add(removeTypeParameters(type.getTypeSignature()));
        });
        this.constantTypes = constantTypes.build();
    }

    public Set<TypeSignature> getConstantTypes()
    {
        return constantTypes;
    }

    public boolean constantTypesContains(TypeSignature typeSignature)
    {
        return constantTypes.contains(typeSignature);
    }
}
