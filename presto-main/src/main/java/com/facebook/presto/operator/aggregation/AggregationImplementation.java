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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionKind;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.LongVariableConstraint;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.TypeVariableConstraint;
import com.facebook.presto.operator.ParametricImplementation;
import com.facebook.presto.operator.annotations.ImplementationDependency;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.LiteralParameters;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.type.Constraint;
import com.facebook.presto.type.LiteralParameter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.facebook.presto.operator.annotations.AnnotationHelpers.containsAnnotation;
import static com.facebook.presto.operator.annotations.AnnotationHelpers.containsImplementationDependencyAnnotation;
import static com.facebook.presto.operator.annotations.AnnotationHelpers.createTypeVariableConstraints;
import static com.facebook.presto.operator.annotations.ImplementationDependency.Factory.createDependency;
import static com.facebook.presto.operator.annotations.ImplementationDependency.isImplementationDependencyAnnotation;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class AggregationImplementation
        implements ParametricImplementation
{
    static Optional<String> getDescription(AnnotatedElement base, AnnotatedElement override)
    {
        Description description = override.getAnnotation(Description.class);
        if (description != null) {
            return Optional.of(description.value());
        }
        description = base.getAnnotation(Description.class);
        return (description == null) ? Optional.empty() : Optional.of(description.value());
    }

    static Optional<String> getDescription(AnnotatedElement base)
    {
        Description description = base.getAnnotation(Description.class);
        return (description == null) ? Optional.empty() : Optional.of(description.value());
    }

    public static class AggregateNativeContainerType
    {
        private final Class<?> javaType;
        private final boolean isBlockPosition;

        public AggregateNativeContainerType(Class<?> javaType, boolean isBlockPosition)
        {
            this.javaType = javaType;
            this.isBlockPosition = isBlockPosition;
        }

        public Class<?> getJavaType()
        {
            return javaType;
        }

        public boolean isBlockPosition()
        {
            return isBlockPosition;
        }
    }

    private final Signature signature;

    private final Class<?> definitionClass;
    private final Class<?> stateClass;
    private final Method inputFunction;
    private final Method outputFunction;
    private final Optional<Method> stateSerializerFactory;
    private final List<AggregateNativeContainerType> argumentNativeContainerTypes;
    private final List<ImplementationDependency> inputDependencies;
    private final List<ImplementationDependency> combineDependencies;
    private final List<ImplementationDependency> outputDependencies;
    private final List<ImplementationDependency> stateSerializerFactoryDependencies;

    public AggregationImplementation(
            Signature signature,
            Class<?> definitionClass,
            Class<?> stateClass,
            Method inputFunction,
            Method outputFunction,
            Optional<Method> stateSerializerFactory,
            List<AggregateNativeContainerType> argumentNativeContainerTypes,
            List<ImplementationDependency> inputDependencies,
            List<ImplementationDependency> combineDependencies,
            List<ImplementationDependency> outputDependencies,
            List<ImplementationDependency> stateSerializerFactoryDependencies)
    {
        this.signature = requireNonNull(signature, "signature cannot be null");
        this.definitionClass = requireNonNull(definitionClass, "definition class cannot be null");
        this.stateClass = requireNonNull(stateClass, "stateClass cannot be null");
        this.inputFunction = requireNonNull(inputFunction, "inputFunction cannot be null");
        this.outputFunction = requireNonNull(outputFunction, "outputFunction cannot be null");
        this.stateSerializerFactory = stateSerializerFactory;
        this.argumentNativeContainerTypes = requireNonNull(argumentNativeContainerTypes, "argumentNativeContainerTypes cannot be null");
        this.inputDependencies = requireNonNull(inputDependencies, "inputDependencies cannot be null");
        this.outputDependencies = requireNonNull(outputDependencies, "outputDependencies cannot be null");
        this.combineDependencies = requireNonNull(combineDependencies, "combineDependencies cannot be null");
        this.stateSerializerFactoryDependencies = stateSerializerFactoryDependencies;
    }

    @Override
    public Signature getSignature()
    {
        return signature;
    }

    @Override
    public boolean hasSpecializedTypeParameters()
    {
        return false;
    }

    public Class<?> getDefinitionClass()
    {
        return definitionClass;
    }

    public Class<?> getStateClass()
    {
        return stateClass;
    }

    public Method getInputFunction()
    {
        return inputFunction;
    }

    public Method getOutputFunction()
    {
        return outputFunction;
    }

    public List<ImplementationDependency> getInputDependencies()
    {
        return inputDependencies;
    }

    public List<ImplementationDependency> getOutputDependencies()
    {
        return outputDependencies;
    }

    public List<ImplementationDependency> getCombineDependencies()
    {
        return combineDependencies;
    }

    public Optional<Method> getStateSerializerFactory()
    {
        return stateSerializerFactory;
    }

    public List<ImplementationDependency> getStateSerializerFactoryDependencies()
    {
        return stateSerializerFactoryDependencies;
    }

    public boolean areTypesAssignable(Signature boundSignature, BoundVariables variables, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        checkState(argumentNativeContainerTypes.size() == boundSignature.getArgumentTypes().size(), "Number of argument assigned to AggregationImplementation is different than number parsed from annotations.");

        // TODO specialized functions variants support is missing here
        for (int i = 0; i < boundSignature.getArgumentTypes().size(); i++) {
            Class<?> argumentType = typeManager.getType(boundSignature.getArgumentTypes().get(i)).getJavaType();
            Class<?> methodDeclaredType = argumentNativeContainerTypes.get(i).getJavaType();
            boolean isCurrentBlockPosition = argumentNativeContainerTypes.get(i).isBlockPosition();

            if (isCurrentBlockPosition && Block.class.isAssignableFrom(methodDeclaredType)) {
                continue;
            }
            if (!isCurrentBlockPosition && argumentType.isAssignableFrom(methodDeclaredType)) {
                continue;
            }
            return false;
        }

        return true;
    }

    public static final class Parser
    {
        private Parser(String name, Method inputMethod, Method combineMethod, Method outputMethod) // TODO Add constructors support
        {
        }

        public static AggregationImplementation parseImplementation(Class<?> aggregationDefinition, AggregationHeader header, Class<?> stateClass, Method inputFunction, Method combineFunction, Method outputFunction, Optional<Method> stateSerializerFactoryFunction)
        {
            List<ImplementationDependency> inputDependencies = parseImplementationDependencies(inputFunction);
            List<ImplementationDependency> combineDependencies = parseImplementationDependencies(combineFunction);
            List<ImplementationDependency> outputDependencies = parseImplementationDependencies(outputFunction);
            List<ImplementationDependency> stateSerializerFactoryDependencies = stateSerializerFactoryFunction.isPresent() ? parseImplementationDependencies(stateSerializerFactoryFunction.get()) : ImmutableList.of();
            List<LongVariableConstraint> longVariableConstraints = parseLongVariableConstraints(inputFunction);
            List<TypeVariableConstraint> typeVariableConstraints = parseTypeVariableConstraints(inputFunction, inputDependencies);
            List<AggregateNativeContainerType> signatureArgumentsTypes = parseSignatureArgumentsTypes(inputFunction);

            List<TypeSignature> inputTypes = getInputTypesSignatures(inputFunction);
            TypeSignature outputType = TypeSignature.parseTypeSignature(outputFunction.getAnnotation(OutputFunction.class).value());

            Signature signature = new Signature(
                    header.getName(),
                    FunctionKind.AGGREGATE,
                    typeVariableConstraints,
                    longVariableConstraints,
                    outputType,
                    inputTypes,
                    false);

            return new AggregationImplementation(signature, aggregationDefinition, stateClass, inputFunction, outputFunction, stateSerializerFactoryFunction, signatureArgumentsTypes, inputDependencies, combineDependencies, outputDependencies, stateSerializerFactoryDependencies);
        }

        public static List<AggregateNativeContainerType> parseSignatureArgumentsTypes(Method inputFunction)
        {
            ImmutableList.Builder<AggregateNativeContainerType> builder = ImmutableList.builder();

            int stateId = findAggregationStateParamId(inputFunction);

            for (int i = 0; i < inputFunction.getParameterCount(); i++) {
                Class<?> parameterType = inputFunction.getParameterTypes()[i];
                Annotation[] annotations = inputFunction.getParameterAnnotations()[i];

                // Skip injected parameters
                if (parameterType == ConnectorSession.class) {
                    continue;
                }

                if (containsAnnotation(annotations, Parser::isAggregationMetaAnnotation)) {
                    continue;
                }

                builder.add(new AggregateNativeContainerType(inputFunction.getParameterTypes()[i], isParameterBlock(annotations)));
            }

            return builder.build();
        }

        public static List<TypeVariableConstraint> parseTypeVariableConstraints(Method inputFunction, List<ImplementationDependency> dependencies)
        {
            return createTypeVariableConstraints(Arrays.asList(inputFunction.getAnnotationsByType(TypeParameter.class)), dependencies);
        }

        public static List<ImplementationDependency> parseImplementationDependencies(Method inputFunction)
        {
            ImmutableList.Builder<ImplementationDependency> builder = ImmutableList.builder();
            List<TypeParameter> typeParameters = Arrays.asList(inputFunction.getAnnotationsByType(TypeParameter.class));
            Set<String> literalParameters = getLiteralParameters(inputFunction);

            for (int i = 0; i < inputFunction.getParameterCount(); i++) {
                Class<?> parameterType = inputFunction.getParameterTypes()[i];
                Annotation[] annotations = inputFunction.getParameterAnnotations()[i];

                // Skip injected parameters
                if (parameterType == ConnectorSession.class) {
                    continue;
                }

                if (containsImplementationDependencyAnnotation(annotations)) {
                    checkArgument(annotations.length == 1, "Meta parameters may only have a single annotation [%s]", inputFunction);
                    Annotation annotation = annotations[0];
                    if (annotation instanceof TypeParameter) {
                        checkArgument(typeParameters.contains(annotation), "Injected type parameters must be declared with @TypeParameter annotation on the method [%s]", inputFunction);
                    }
                    if (annotation instanceof LiteralParameter) {
                        checkArgument(literalParameters.contains(((LiteralParameter) annotation).value()), "Parameter injected by @LiteralParameter must be declared with @LiteralParameters on the method [%s]", inputFunction);
                    }
                    builder.add(createDependency(annotation, literalParameters));
                }
            }
            return builder.build();
        }

        public static List<LongVariableConstraint> parseLongVariableConstraints(Method inputFunction)
        {
            return Stream.of(inputFunction.getAnnotationsByType(Constraint.class))
                    .map(annotation -> new LongVariableConstraint(annotation.variable(), annotation.expression()))
                    .collect(toImmutableList());
        }

        public static boolean isParameterNullable(Annotation[] annotations)
        {
            return containsAnnotation(annotations, annotation -> annotation instanceof NullablePosition);
        }

        public static boolean isParameterBlock(Annotation[] annotations)
        {
            return containsAnnotation(annotations, annotation -> annotation instanceof BlockPosition);
        }

        public static List<TypeSignature> getInputTypesSignatures(Method inputFunction)
        {
            // FIXME Literal parameters should be part of class annotations.
            ImmutableList.Builder<TypeSignature> builder = ImmutableList.builder();
            Set<String> literalParameters = getLiteralParameters(inputFunction);

            Annotation[][] parameterAnnotations = inputFunction.getParameterAnnotations();
            for (Annotation[] annotations : parameterAnnotations) {
                for (Annotation annotation : annotations) {
                    if (annotation instanceof SqlType) {
                        String typeName = ((SqlType) annotation).value();
                        builder.add(parseTypeSignature(typeName, literalParameters));
                    }
                }
            }

            return builder.build();
        }

        public static Class<?> findAggregationStateParamType(Method inputFunction)
        {
            return inputFunction.getParameterTypes()[findAggregationStateParamId(inputFunction)];
        }

        public static int findAggregationStateParamId(Method method)
        {
            return findAggregationStateParamId(method, 0);
        }

        public static int findAggregationStateParamId(Method method, int id)
        {
            int currentParamId = 0;
            int found = 0;
            for (Annotation[] annotations : method.getParameterAnnotations()) {
                for (Annotation annotation : annotations) {
                    if (annotation instanceof AggregationState) {
                        if (found++ == id) {
                            return currentParamId;
                        }
                    }
                }
                currentParamId++;
            }

            // backward compatibility @AggregationState annotation didn't exists before
            // some third party aggregates may assume that State will be id-th parameter
            return id;
        }

        public static Set<String> getLiteralParameters(Method inputFunction)
        {
            ImmutableSet.Builder<String> literalParametersBuilder = ImmutableSet.builder();

            Annotation[] literalParameters = inputFunction.getAnnotations();
            for (Annotation annotation : literalParameters) {
                if (annotation instanceof LiteralParameters) {
                    for (String literal : ((LiteralParameters) annotation).value()) {
                       literalParametersBuilder.add(literal);
                    }
                }
            }

            return literalParametersBuilder.build();
        }

        public static boolean isAggregationMetaAnnotation(Annotation annotation)
        {
            return annotation instanceof BlockIndex || annotation instanceof AggregationState || isImplementationDependencyAnnotation(annotation);
        }
    }
}
