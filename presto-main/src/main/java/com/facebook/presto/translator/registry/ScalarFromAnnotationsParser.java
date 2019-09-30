package com.facebook.presto.translator.registry;

import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.ScalarOperator;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.lang.annotation.Annotation;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static com.facebook.presto.spi.function.FunctionKind.SCALAR;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.translator.registry.ScalarHeader.fromAnnotatedElement;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ScalarFromAnnotationsParser
{
    private ScalarFromAnnotationsParser()
    {
    }

    public static Map<FunctionMetadata, MethodHandle> parseFunctionDefinitions(Class<?> clazz)
    {
        ImmutableMap.Builder<FunctionMetadata, MethodHandle> translatorBuilder = ImmutableMap.builder();
        for (ScalarHeaderAndMethods methods : findScalarsFromSetClass(clazz)) {
            translatorBuilder.putAll(scalarToFunctionMetadata(methods));
        }

        return translatorBuilder.build();
    }

    public static TypeSignature removeTypeParameters(TypeSignature typeSignature)
    {
        return new TypeSignature(typeSignature.getBase());
    }

    public static FunctionMetadata removeTypeParameters(FunctionMetadata metadata)
    {
        ImmutableList.Builder<TypeSignature> argumentsBuilder = ImmutableList.builder();
        for (TypeSignature typeSignature : metadata.getArgumentTypes()) {
            argumentsBuilder.add(removeTypeParameters(typeSignature));
        }

        if (metadata.getOperatorType().isPresent()) {
            return new FunctionMetadata(
                    metadata.getOperatorType().get(),
                    argumentsBuilder.build(),
                    metadata.getReturnType(),
                    metadata.getFunctionKind(),
                    metadata.isDeterministic(),
                    metadata.isCalledOnNullInput());
        }
        else {
            return new FunctionMetadata(
                    metadata.getName(),
                    argumentsBuilder.build(),
                    metadata.getReturnType(),
                    metadata.getFunctionKind(),
                    metadata.isDeterministic(),
                    metadata.isCalledOnNullInput());
        }
    }

    private static MethodHandle getMethodHandle(Method method)
    {
        try {
            return MethodHandles.lookup().unreflect(method);
        }
        catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private static List<ScalarHeaderAndMethods> findScalarsFromSetClass(Class<?> clazz)
    {
        ImmutableList.Builder<ScalarHeaderAndMethods> builder = ImmutableList.builder();
        for (Method method : findPublicMethodsWithAnnotation(clazz, SqlType.class, ScalarFunction.class, ScalarOperator.class)) {
            boolean annotationCondition = (method.getAnnotation(ScalarFunction.class) != null) || (method.getAnnotation(ScalarOperator.class) != null);
            checkArgument(annotationCondition, format("Method [%s] annotated with @SqlType is missing @ScalarFunction or @ScalarOperator", method));

            for (ScalarHeader header : fromAnnotatedElement(method)) {
                builder.add(new ScalarHeaderAndMethods(header, ImmutableSet.of(method)));
            }
        }
        List<ScalarHeaderAndMethods> methods = builder.build();
        checkArgument(!methods.isEmpty(), "Class [%s] does not have any methods annotated with @ScalarFunction or @ScalarOperator", clazz.getName());

        return methods;
    }

    private static Map<FunctionMetadata, MethodHandle> scalarToFunctionMetadata(ScalarHeaderAndMethods scalar)
    {
        ScalarHeader header = scalar.getHeader();

        ImmutableMap.Builder<FunctionMetadata, MethodHandle> metadataBuilder = ImmutableMap.builder();
        for (Method method : scalar.getMethods()) {
            FunctionMetadata metadata = methodToFunctionMetadata(header, method);
            metadataBuilder.put(removeTypeParameters(metadata), getMethodHandle(method));
        }

        return metadataBuilder.build();
    }

    private static FunctionMetadata methodToFunctionMetadata(ScalarHeader header, Method method)
    {
        requireNonNull(header, "header is null");

        // return type
        SqlType annotatedReturnType = method.getAnnotation(SqlType.class);
        checkArgument(annotatedReturnType != null, format("Method [%s] is missing @SqlType annotation", method));
        TypeSignature returnType = parseTypeSignature(annotatedReturnType.value(), ImmutableSet.of());

        // argument type
        ImmutableList.Builder<TypeSignature> argumentTypes = new ImmutableList.Builder<>();
        for (Parameter parameter : method.getParameters()) {
            Annotation[] annotations = parameter.getAnnotations();

            SqlType type = Stream.of(annotations)
                    .filter(SqlType.class::isInstance)
                    .map(SqlType.class::cast)
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException(format("Method [%s] is missing @SqlType annotation for parameter", method)));
            TypeSignature typeSignature = parseTypeSignature(type.value(), ImmutableSet.of());
            argumentTypes.add(typeSignature);
        }

        if (header.getOperatorType().isPresent()) {
            return new FunctionMetadata(header.getOperatorType().get(), argumentTypes.build(), returnType, SCALAR, header.isDeterministic(), header.isCalledOnNullInput());
        }
        else {
            return new FunctionMetadata(header.getName(), argumentTypes.build(), returnType, SCALAR, header.isDeterministic(), header.isCalledOnNullInput());
        }
    }

    @SafeVarargs
    private static Set<Method> findPublicMethodsWithAnnotation(Class<?> clazz, Class<? extends Annotation>... annotationClasses)
    {
        ImmutableSet.Builder<Method> methods = ImmutableSet.builder();
        for (Method method : clazz.getDeclaredMethods()) {
            for (Annotation annotation : method.getAnnotations()) {
                for (Class<?> annotationClass : annotationClasses) {
                    if (annotationClass.isInstance(annotation)) {
                        checkArgument(Modifier.isPublic(method.getModifiers()), "Method [%s] annotated with @%s must be public", method, annotationClass.getSimpleName());
                        methods.add(method);
                    }
                }
            }
        }
        return methods.build();
    }

    private static class ScalarHeaderAndMethods
    {
        private final ScalarHeader header;
        private final Set<Method> methods;

        public ScalarHeaderAndMethods(ScalarHeader header, Set<Method> methods)
        {
            this.header = requireNonNull(header, "header is null");
            this.methods = requireNonNull(methods, "methods are null");
        }

        public ScalarHeader getHeader()
        {
            return header;
        }

        public Set<Method> getMethods()
        {
            return methods;
        }
    }
}
