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

import com.facebook.presto.bytecode.BytecodeBlock;
import com.facebook.presto.bytecode.CallSiteBinder;
import com.facebook.presto.bytecode.ClassDefinition;
import com.facebook.presto.bytecode.MethodDefinition;
import com.facebook.presto.bytecode.Parameter;
import com.facebook.presto.bytecode.Scope;
import com.facebook.presto.bytecode.Variable;
import com.facebook.presto.bytecode.control.IfStatement;
import com.facebook.presto.common.NotSupportedException;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.ComplexTypeFunctionDescriptor;
import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.function.LambdaArgumentDescriptor;
import com.facebook.presto.spi.function.LambdaDescriptor;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlFunctionVisibility;
import com.facebook.presto.sql.gen.lambda.UnaryFunctionInterface;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Primitives;
import it.unimi.dsi.fastutil.ints.IntComparator;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.Optional;

import static com.facebook.presto.bytecode.Access.FINAL;
import static com.facebook.presto.bytecode.Access.PUBLIC;
import static com.facebook.presto.bytecode.Access.a;
import static com.facebook.presto.bytecode.Parameter.arg;
import static com.facebook.presto.bytecode.ParameterizedType.type;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantNull;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.equal;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.common.type.UnknownType.UNKNOWN;
import static com.facebook.presto.metadata.BuiltInTypeAndFunctionNamespaceManager.JAVA_BUILTIN_NAMESPACE;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.ArgumentProperty.functionTypeArgumentProperty;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.ArgumentProperty.valueTypeArgumentProperty;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.NullConvention.RETURN_NULL_ON_NULL;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.function.Signature.typeVariable;
import static com.facebook.presto.sql.gen.SqlTypeBytecodeExpression.constantType;
import static com.facebook.presto.util.CompilerUtils.defineClass;
import static com.facebook.presto.util.CompilerUtils.makeClassName;
import static com.facebook.presto.util.Reflection.methodHandle;
import static it.unimi.dsi.fastutil.ints.IntArrays.quickSort;

public abstract class AbstractArraySortByKeyFunction
        extends SqlScalarFunction
{
    private final ComplexTypeFunctionDescriptor descriptor;

    protected AbstractArraySortByKeyFunction(String functionName)
    {
        super(new Signature(
                QualifiedObjectName.valueOf(JAVA_BUILTIN_NAMESPACE, functionName),
                FunctionKind.SCALAR,
                ImmutableList.of(typeVariable("T"), typeVariable("K")),
                ImmutableList.of(),
                parseTypeSignature("array(T)"),
                ImmutableList.of(parseTypeSignature("array(T)"), parseTypeSignature("function(T,K)")),
                false));
        descriptor = new ComplexTypeFunctionDescriptor(
                true,
                ImmutableList.of(new LambdaDescriptor(1, ImmutableMap.of(0, new LambdaArgumentDescriptor(0, ComplexTypeFunctionDescriptor::prependAllSubscripts)))),
                Optional.of(ImmutableSet.of(0)),
                Optional.of(ComplexTypeFunctionDescriptor::clearRequiredSubfields),
                getSignature());
    }

    @Override
    public SqlFunctionVisibility getVisibility()
    {
        return SqlFunctionVisibility.PUBLIC;
    }

    @Override
    public boolean isDeterministic()
    {
        return true;
    }

    @Override
    public BuiltInScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
    {
        Type elementType = boundVariables.getTypeVariable("T");
        Type keyType = boundVariables.getTypeVariable("K");

        // Generate the specialized key extractor instance once
        KeyExtractor keyExtractor = generateKeyExtractor(elementType, keyType);

        MethodHandle raw = methodHandle(
                AbstractArraySortByKeyFunction.class,
                "sortByKey",
                AbstractArraySortByKeyFunction.class,
                Type.class,
                Type.class,
                KeyExtractor.class,
                SqlFunctionProperties.class,
                Block.class,
                UnaryFunctionInterface.class);

        MethodHandle bound = MethodHandles.insertArguments(raw, 0, this, elementType, keyType, keyExtractor);

        return new BuiltInScalarFunctionImplementation(
                false,
                ImmutableList.of(
                        valueTypeArgumentProperty(RETURN_NULL_ON_NULL),  // array parameter
                        functionTypeArgumentProperty(UnaryFunctionInterface.class)),  // keyFunction parameter
                bound);
    }

    @Override
    public ComplexTypeFunctionDescriptor getComplexTypeFunctionDescriptor()
    {
        return descriptor;
    }

    public static Block sortByKey(
            AbstractArraySortByKeyFunction function,
            Type elementType,
            Type keyType,
            KeyExtractor keyExtractor,
            SqlFunctionProperties properties,
            Block array,
            UnaryFunctionInterface keyFunction)
    {
        int arrayLength = array.getPositionCount();
        if (arrayLength < 2) {
            return array;
        }

        // Create array of indices and extracted keys
        int[] indices = new int[arrayLength];
        BlockBuilder keyBlockBuilder = keyType.createBlockBuilder(null, arrayLength);

        // Extract keys for all elements
        for (int i = 0; i < arrayLength; i++) {
            indices[i] = i;
            if (array.isNull(i)) {
                keyBlockBuilder.appendNull();
            }
            else {
                try {
                    // Use the generated KeyExtractor implementation (direct virtual call)
                    keyExtractor.extract(properties, array, i, keyFunction, keyBlockBuilder);
                }
                catch (Throwable t) {
                    throw new PrestoException(INVALID_FUNCTION_ARGUMENT, String.format("Error applying key function to element at position %d", i), t);
                }
            }
        }

        Block keysBlock = keyBlockBuilder.build();

        // Sort indices based on extracted keys using Type's compareTo
        try {
            if (array.mayHaveNull() || keysBlock.mayHaveNull()) {
                quickSort(indices, new NullableComparator(array, keysBlock, keyType, function));
            }
            else {
                quickSort(indices, new NonNullableComparator(keysBlock, keyType, function));
            }
        }
        catch (NotSupportedException | UnsupportedOperationException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Key type does not support comparison", e);
        }
        catch (PrestoException e) {
            if (e.getErrorCode() == NOT_SUPPORTED.toErrorCode()) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Key type does not support comparison", e);
            }
            throw e;
        }

        // Build result block with sorted elements
        BlockBuilder resultBuilder = elementType.createBlockBuilder(null, arrayLength);
        for (int i = 0; i < arrayLength; i++) {
            elementType.appendTo(array, indices[i], resultBuilder);
        }

        return resultBuilder.build();
    }

    /**
     * KeyExtractor is a simple interface implemented by generated classes.
     * Implementations must write the extracted key into the provided BlockBuilder
     * (or appendNull) for the given position.
     */
    public interface KeyExtractor
    {
        void extract(SqlFunctionProperties properties, Block array, int position, UnaryFunctionInterface keyFunction, BlockBuilder keyBlockBuilder) throws Throwable;
    }

    // Generate just the key extraction logic
    public static KeyExtractor generateKeyExtractor(Type elementType, Type keyType)
    {
        CallSiteBinder binder = new CallSiteBinder();
        Class<?> elementJavaType = Primitives.wrap(elementType.getJavaType());
        Class<?> keyJavaType = Primitives.wrap(keyType.getJavaType());

        String className = "ArraySortKeyExtractorImpl_" + elementType.getTypeSignature() + "_" + keyType.getTypeSignature();
        ClassDefinition definition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName(className),
                type(Object.class),
                type(KeyExtractor.class));
        definition.declareDefaultConstructor(a(PUBLIC));

        Parameter properties = arg("properties", SqlFunctionProperties.class);
        Parameter array = arg("array", Block.class);
        Parameter position = arg("position", int.class);
        Parameter keyFunction = arg("keyFunction", UnaryFunctionInterface.class);
        Parameter keyBlockBuilder = arg("keyBlockBuilder", BlockBuilder.class);

        MethodDefinition method = definition.declareMethod(
                a(PUBLIC),
                "extract",
                type(void.class),
                ImmutableList.of(properties, array, position, keyFunction, keyBlockBuilder));

        BytecodeBlock body = method.getBody();
        Scope scope = method.getScope();
        Variable element = scope.declareVariable(elementJavaType, "element");
        Variable key = scope.declareVariable(keyJavaType, "key");

        // Load element with correct primitive handling
        if (!elementType.equals(UNKNOWN)) {
            // generates the correct getLong/getDouble/getBoolean/getSlice/getObject call
            body.append(element.set(constantType(binder, elementType).getValue(array, position).cast(elementJavaType)));
        }
        else {
            body.append(element.set(constantNull(elementJavaType)));
        }

        body.append(key.set(keyFunction.invoke("apply", Object.class, element.cast(Object.class)).cast(keyJavaType)));

        // Write the key to the block builder
        if (!keyType.equals(UNKNOWN)) {
            body.append(new IfStatement()
                    .condition(equal(key, constantNull(keyJavaType)))
                    .ifTrue(keyBlockBuilder.invoke("appendNull", BlockBuilder.class).pop())
                    .ifFalse(constantType(binder, keyType).writeValue(keyBlockBuilder, key.cast(keyType.getJavaType()))));
        }
        else {
            body.append(keyBlockBuilder.invoke("appendNull", BlockBuilder.class).pop());
        }

        body.ret();

        Class<?> generatedClass = defineClass(definition, Object.class, binder.getBindings(), AbstractArraySortByKeyFunction.class.getClassLoader());

        try {
            // instantiate generated class and cast to KeyExtractor for direct virtual call
            return (KeyExtractor) generatedClass.getConstructor().newInstance();
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException("Failed to instantiate generated key extractor", e);
        }
    }

    // Abstract method to be implemented by subclasses to define comparison direction
    protected abstract int compareKeys(Type keyType, Block keysBlock, int leftIndex, int rightIndex);

    private static class NullableComparator
            implements IntComparator
    {
        private final Block array;
        private final Block keysBlock;
        private final Type keyType;
        private final AbstractArraySortByKeyFunction function;

        public NullableComparator(Block array, Block keysBlock, Type keyType, AbstractArraySortByKeyFunction function)
        {
            this.array = array;
            this.keysBlock = keysBlock;
            this.keyType = keyType;
            this.function = function;
        }

        @Override
        public int compare(int leftIndex, int rightIndex)
        {
            boolean leftArrayNull = array.isNull(leftIndex);
            boolean rightArrayNull = array.isNull(rightIndex);

            if (leftArrayNull && rightArrayNull) {
                return 0;
            }
            if (leftArrayNull) {
                return 1;
            }
            if (rightArrayNull) {
                return -1;
            }

            boolean leftKeyNull = keysBlock.isNull(leftIndex);
            boolean rightKeyNull = keysBlock.isNull(rightIndex);

            if (leftKeyNull && rightKeyNull) {
                return 0;
            }
            if (leftKeyNull) {
                return 1;
            }
            if (rightKeyNull) {
                return -1;
            }

            int result = function.compareKeys(keyType, keysBlock, leftIndex, rightIndex);

            // If keys are equal, maintain original order
            if (result == 0) {
                return Integer.compare(leftIndex, rightIndex);
            }

            return result;
        }
    }

    private static class NonNullableComparator
            implements IntComparator
    {
        private final Block keysBlock;
        private final Type keyType;
        private final AbstractArraySortByKeyFunction function;

        public NonNullableComparator(Block keysBlock, Type keyType, AbstractArraySortByKeyFunction function)
        {
            this.keysBlock = keysBlock;
            this.keyType = keyType;
            this.function = function;
        }

        @Override
        public int compare(int leftIndex, int rightIndex)
        {
            int result = function.compareKeys(keyType, keysBlock, leftIndex, rightIndex);

            // If keys are equal, maintain original order
            if (result == 0) {
                return Integer.compare(leftIndex, rightIndex);
            }

            return result;
        }
    }

    public static class ArraySortByKeyFunction
            extends AbstractArraySortByKeyFunction
    {
        public static final ArraySortByKeyFunction ARRAY_SORT_BY_KEY_FUNCTION = new ArraySortByKeyFunction();

        private ArraySortByKeyFunction()
        {
            super("array_sort");
        }

        @Override
        public String getDescription()
        {
            return "Sorts the given array using a lambda function to extract sorting keys. " +
                    "Null array elements and null keys are placed at the end. " +
                    "Example: array_sort(ARRAY['apple', 'banana', 'cherry'], x -> length(x))";
        }

        @Override
        protected int compareKeys(Type keyType, Block keysBlock, int leftIndex, int rightIndex)
        {
            return keyType.compareTo(keysBlock, leftIndex, keysBlock, rightIndex);
        }
    }

    public static class ArraySortDescByKeyFunction
            extends AbstractArraySortByKeyFunction
    {
        public static final ArraySortDescByKeyFunction ARRAY_SORT_DESC_BY_KEY_FUNCTION = new ArraySortDescByKeyFunction();

        private ArraySortDescByKeyFunction()
        {
            super("array_sort_desc");
        }

        @Override
        public String getDescription()
        {
            return "Sorts the given array in descending order using a lambda function to extract sorting keys";
        }

        @Override
        protected int compareKeys(Type keyType, Block keysBlock, int leftIndex, int rightIndex)
        {
            return keyType.compareTo(keysBlock, rightIndex, keysBlock, leftIndex);
        }
    }
}
