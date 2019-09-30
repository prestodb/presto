package com.facebook.presto.translator.registry;

import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.ScalarOperator;
import com.facebook.presto.spi.relation.FullyQualifiedName;
import com.google.common.collect.ImmutableList;

import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.CaseFormat.LOWER_CAMEL;
import static com.google.common.base.CaseFormat.LOWER_UNDERSCORE;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class ScalarHeader
{
    private final FullyQualifiedName name;
    private final Optional<OperatorType> operatorType;
    private final boolean deterministic;
    private final boolean calledOnNullInput;

    private ScalarHeader(String name, boolean deterministic, boolean calledOnNullInput)
    {
        // TODO This is a hack. Engine should provide an API for connectors to overwrite functions. Connector should not hard code the builtin function namespace.
        this.name = requireNonNull(FullyQualifiedName.of("presto", "default", name));
        this.operatorType = Optional.empty();
        this.deterministic = deterministic;
        this.calledOnNullInput = calledOnNullInput;
    }

    private ScalarHeader(OperatorType operatorType, boolean deterministic, boolean calledOnNullInput)
    {
        this.name = operatorType.getFunctionName();
        this.operatorType = Optional.of(operatorType);
        this.deterministic = deterministic;
        this.calledOnNullInput = calledOnNullInput;
    }

    private static String annotatedName(AnnotatedElement annotatedElement)
    {
        if (annotatedElement instanceof Class<?>) {
            return ((Class<?>) annotatedElement).getSimpleName();
        }
        else if (annotatedElement instanceof Method) {
            return ((Method) annotatedElement).getName();
        }

        throw new UnsupportedOperationException("Only Classes and Methods are supported as annotated elements.");
    }

    private static String camelToSnake(String name)
    {
        return LOWER_CAMEL.to(LOWER_UNDERSCORE, name);
    }

    public static List<ScalarHeader> fromAnnotatedElement(AnnotatedElement annotated)
    {
        ScalarFunction scalarFunction = annotated.getAnnotation(ScalarFunction.class);
        ScalarOperator scalarOperator = annotated.getAnnotation(ScalarOperator.class);

        ImmutableList.Builder<ScalarHeader> builder = ImmutableList.builder();

        if (scalarFunction != null) {
            String baseName = scalarFunction.value().isEmpty() ? camelToSnake(annotatedName(annotated)) : scalarFunction.value();
            builder.add(new ScalarHeader(baseName, scalarFunction.deterministic(), scalarFunction.calledOnNullInput()));

            for (String alias : scalarFunction.alias()) {
                builder.add(new ScalarHeader(alias, scalarFunction.deterministic(), scalarFunction.calledOnNullInput()));
            }
        }

        if (scalarOperator != null) {
            builder.add(new ScalarHeader(scalarOperator.value(), true, scalarOperator.value().isCalledOnNullInput()));
        }

        List<ScalarHeader> result = builder.build();
        checkArgument(!result.isEmpty());
        return result;
    }

    public FullyQualifiedName getName()
    {
        return name;
    }

    public Optional<OperatorType> getOperatorType()
    {
        return operatorType;
    }

    public boolean isDeterministic()
    {
        return deterministic;
    }

    public boolean isCalledOnNullInput()
    {
        return calledOnNullInput;
    }
}
