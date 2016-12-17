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
package com.facebook.presto.operator.aggregation.state;

import com.facebook.presto.array.BlockBigArray;
import com.facebook.presto.array.BooleanBigArray;
import com.facebook.presto.array.ByteBigArray;
import com.facebook.presto.array.DoubleBigArray;
import com.facebook.presto.array.LongBigArray;
import com.facebook.presto.array.SliceBigArray;
import com.facebook.presto.bytecode.BytecodeBlock;
import com.facebook.presto.bytecode.ClassDefinition;
import com.facebook.presto.bytecode.DynamicClassLoader;
import com.facebook.presto.bytecode.FieldDefinition;
import com.facebook.presto.bytecode.MethodDefinition;
import com.facebook.presto.bytecode.Parameter;
import com.facebook.presto.bytecode.Scope;
import com.facebook.presto.bytecode.Variable;
import com.facebook.presto.bytecode.expression.BytecodeExpression;
import com.facebook.presto.operator.aggregation.GroupedAccumulator;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import com.facebook.presto.spi.function.AccumulatorStateMetadata;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.gen.CallSiteBinder;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Ordering;
import io.airlift.slice.SizeOf;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.openjdk.jol.info.ClassLayout;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.bytecode.Access.FINAL;
import static com.facebook.presto.bytecode.Access.PRIVATE;
import static com.facebook.presto.bytecode.Access.PUBLIC;
import static com.facebook.presto.bytecode.Access.STATIC;
import static com.facebook.presto.bytecode.Access.a;
import static com.facebook.presto.bytecode.CompilerUtils.defineClass;
import static com.facebook.presto.bytecode.CompilerUtils.makeClassName;
import static com.facebook.presto.bytecode.Parameter.arg;
import static com.facebook.presto.bytecode.ParameterizedType.type;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.add;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantBoolean;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantInt;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantLong;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantNumber;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.defaultValue;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.invokeStatic;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.newInstance;
import static com.facebook.presto.operator.aggregation.state.StateCompilerUtils.getBlockBuilderAppend;
import static com.facebook.presto.operator.aggregation.state.StateCompilerUtils.getBlockGetter;
import static com.facebook.presto.operator.aggregation.state.StateCompilerUtils.getSliceGetter;
import static com.facebook.presto.operator.aggregation.state.StateCompilerUtils.getSliceSetter;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.sql.gen.SqlTypeBytecodeExpression.constantType;
import static com.google.common.base.CaseFormat.LOWER_CAMEL;
import static com.google.common.base.CaseFormat.UPPER_CAMEL;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class StateCompiler
{
    private StateCompiler()
    {
    }

    private static Class<?> getBigArrayType(Class<?> type)
    {
        if (type.equals(long.class)) {
            return LongBigArray.class;
        }
        if (type.equals(byte.class)) {
            return ByteBigArray.class;
        }
        if (type.equals(double.class)) {
            return DoubleBigArray.class;
        }
        if (type.equals(boolean.class)) {
            return BooleanBigArray.class;
        }
        if (type.equals(Slice.class)) {
            return SliceBigArray.class;
        }
        if (type.equals(Block.class)) {
            return BlockBigArray.class;
        }
        // TODO: support more reference types
        throw new IllegalArgumentException("Unsupported type: " + type.getName());
    }

    public static <T> AccumulatorStateSerializer<T> generateStateSerializer(Class<T> clazz)
    {
        return generateStateSerializer(clazz, new DynamicClassLoader(clazz.getClassLoader()));
    }

    public static <T> AccumulatorStateSerializer<T> generateStateSerializer(Class<T> clazz, DynamicClassLoader classLoader)
    {
        AccumulatorStateMetadata metadata = getMetadataAnnotation(clazz);
        if (metadata != null && metadata.stateSerializerClass() != void.class) {
            try {
                return (AccumulatorStateSerializer<T>) metadata.stateSerializerClass().getConstructor().newInstance();
            }
            catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
                throw Throwables.propagate(e);
            }
        }

        ClassDefinition definition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName(clazz.getSimpleName() + "Serializer"),
                type(Object.class),
                type(AccumulatorStateSerializer.class));

        CallSiteBinder callSiteBinder = new CallSiteBinder();

        // Generate constructor
        definition.declareDefaultConstructor(a(PUBLIC));

        List<StateField> fields = enumerateFields(clazz);
        generateGetSerializedType(definition, fields, callSiteBinder);
        generateSerialize(definition, clazz, fields);
        generateDeserialize(definition, clazz, fields);

        Class<? extends AccumulatorStateSerializer> serializerClass = defineClass(definition, AccumulatorStateSerializer.class, callSiteBinder.getBindings(), classLoader);
        try {
            return (AccumulatorStateSerializer<T>) serializerClass.newInstance();
        }
        catch (InstantiationException | IllegalAccessException e) {
            throw Throwables.propagate(e);
        }
    }

    private static void generateGetSerializedType(ClassDefinition definition, List<StateField> fields, CallSiteBinder callSiteBinder)
    {
        BytecodeBlock body = definition.declareMethod(a(PUBLIC), "getSerializedType", type(Type.class)).getBody();

        Type type;
        if (fields.size() > 1) {
            type = VARBINARY;
        }
        else {
            Class<?> stackType = fields.get(0).getType();
            if (stackType == long.class) {
                type = BIGINT;
            }
            else if (stackType == double.class) {
                type = DOUBLE;
            }
            else if (stackType == boolean.class) {
                type = BOOLEAN;
            }
            else if (stackType == byte.class) {
                type = BIGINT;
            }
            else if (stackType == Slice.class) {
                type = VARBINARY;
            }
            else {
                throw new IllegalArgumentException("Unsupported type: " + stackType);
            }
        }

        body.comment("return %s", type.getTypeSignature())
                .append(constantType(callSiteBinder, type))
                .retObject();
    }

    private static <T> AccumulatorStateMetadata getMetadataAnnotation(Class<T> clazz)
    {
        AccumulatorStateMetadata metadata = clazz.getAnnotation(AccumulatorStateMetadata.class);
        if (metadata != null) {
            return metadata;
        }
        // If the annotation wasn't found, then search the super classes
        for (Class<?> superInterface : clazz.getInterfaces()) {
            metadata = superInterface.getAnnotation(AccumulatorStateMetadata.class);
            if (metadata != null) {
                return metadata;
            }
        }

        return null;
    }

    private static <T> void generateDeserialize(ClassDefinition definition, Class<T> clazz, List<StateField> fields)
    {
        Parameter block = arg("block", Block.class);
        Parameter index = arg("index", int.class);
        Parameter state = arg("state", Object.class);
        MethodDefinition method = definition.declareMethod(a(PUBLIC), "deserialize", type(void.class), block, index, state);

        BytecodeBlock deserializerBody = method.getBody();

        if (fields.size() == 1) {
            Method setter = getSetter(clazz, fields.get(0));
            Method blockGetter = getBlockGetter(setter.getParameterTypes()[0]);
            deserializerBody.append(state.cast(setter.getDeclaringClass()).invoke(setter, invokeStatic(blockGetter, block, index)));
        }
        else {
            Variable slice = method.getScope().declareVariable(Slice.class, "slice");
            deserializerBody.append(slice.set(block.invoke("getSlice", Slice.class, index, constantInt(0), block.invoke("getLength", int.class, index))));

            for (StateField field : fields) {
                Method setter = getSetter(clazz, field);
                Method getter = getSliceGetter(setter.getParameterTypes()[0]);
                int offset = offsetOfField(field, fields);
                deserializerBody.append(state.cast(setter.getDeclaringClass()).invoke(setter, invokeStatic(getter, slice, constantInt(offset))));
            }
        }
        deserializerBody.ret();
    }

    private static <T> void generateSerialize(ClassDefinition definition, Class<T> clazz, List<StateField> fields)
    {
        Parameter state = arg("state", Object.class);
        Parameter out = arg("out", BlockBuilder.class);
        MethodDefinition method = definition.declareMethod(a(PUBLIC), "serialize", type(void.class), state, out);

        BytecodeBlock serializerBody = method.getBody();

        if (fields.size() == 1) {
            Method getter = getGetter(clazz, fields.get(0));
            Method append = getBlockBuilderAppend(getter.getReturnType());
            serializerBody.append(invokeStatic(append, out, state.cast(getter.getDeclaringClass()).invoke(getter)));
        }
        else {
            Variable slice = method.getScope().declareVariable(Slice.class, "slice");
            BytecodeExpression size = constantInt(serializedSizeOf(clazz));
            serializerBody.append(slice.set(invokeStatic(Slices.class, "allocate", Slice.class, size)));

            for (StateField field : fields) {
                Method getter = getGetter(clazz, field);
                Method sliceSetter = getSliceSetter(getter.getReturnType());
                serializerBody.append(invokeStatic(sliceSetter, slice, constantInt(offsetOfField(field, fields)), state.cast(getter.getDeclaringClass()).invoke(getter)));
            }
            serializerBody.append(out.invoke("writeBytes", BlockBuilder.class, slice, constantInt(0), size)
                    .invoke("closeEntry", BlockBuilder.class)
                    .pop());
        }
        serializerBody.ret();
    }

    /**
     * Computes the byte offset to store this field at, when serializing it to a Slice
     */
    private static int offsetOfField(StateField targetField, List<StateField> fields)
    {
        int offset = 0;
        for (StateField field : fields) {
            if (targetField.getName().equals(field.getName())) {
                break;
            }
            offset += field.sizeOfType();
        }

        return offset;
    }

    /**
     * Computes the size in bytes that this state will occupy, when serialized as a Slice
     */
    private static int serializedSizeOf(Class<?> stateClass)
    {
        List<StateField> fields = enumerateFields(stateClass);
        int size = 0;
        for (StateField field : fields) {
            size += field.sizeOfType();
        }
        return size;
    }

    private static Method getSetter(Class<?> clazz, StateField field)
    {
        try {
            return clazz.getMethod(field.getSetterName(), field.getType());
        }
        catch (NoSuchMethodException e) {
            throw Throwables.propagate(e);
        }
    }

    private static Method getGetter(Class<?> clazz, StateField field)
    {
        try {
            return clazz.getMethod(field.getGetterName());
        }
        catch (NoSuchMethodException e) {
            throw Throwables.propagate(e);
        }
    }

    public static <T> AccumulatorStateFactory<T> generateStateFactory(Class<T> clazz)
    {
        return generateStateFactory(clazz, new DynamicClassLoader(clazz.getClassLoader()));
    }

    public static <T> AccumulatorStateFactory<T> generateStateFactory(Class<T> clazz, DynamicClassLoader classLoader)
    {
        AccumulatorStateMetadata metadata = getMetadataAnnotation(clazz);
        if (metadata != null && metadata.stateFactoryClass() != void.class) {
            try {
                return (AccumulatorStateFactory<T>) metadata.stateFactoryClass().getConstructor().newInstance();
            }
            catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
                throw Throwables.propagate(e);
            }
        }

        Class<? extends T> singleStateClass = generateSingleStateClass(clazz, classLoader);
        Class<? extends T> groupedStateClass = generateGroupedStateClass(clazz, classLoader);

        ClassDefinition definition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName(clazz.getSimpleName() + "Factory"),
                type(Object.class),
                type(AccumulatorStateFactory.class));

        // Generate constructor
        definition.declareDefaultConstructor(a(PUBLIC));

        // Generate single state creation method
        definition.declareMethod(a(PUBLIC), "createSingleState", type(Object.class))
                .getBody()
                .newObject(singleStateClass)
                .dup()
                .invokeConstructor(singleStateClass)
                .retObject();

        // Generate grouped state creation method
        definition.declareMethod(a(PUBLIC), "createGroupedState", type(Object.class))
                .getBody()
                .newObject(groupedStateClass)
                .dup()
                .invokeConstructor(groupedStateClass)
                .retObject();

        // Generate getters for state class
        definition.declareMethod(a(PUBLIC), "getSingleStateClass", type(Class.class, singleStateClass))
                .getBody()
                .push(singleStateClass)
                .retObject();

        definition.declareMethod(a(PUBLIC), "getGroupedStateClass", type(Class.class, groupedStateClass))
                .getBody()
                .push(groupedStateClass)
                .retObject();

        Class<? extends AccumulatorStateFactory> factoryClass = defineClass(definition, AccumulatorStateFactory.class, classLoader);
        try {
            return (AccumulatorStateFactory<T>) factoryClass.newInstance();
        }
        catch (InstantiationException | IllegalAccessException e) {
            throw Throwables.propagate(e);
        }
    }

    private static <T> Class<? extends T> generateSingleStateClass(Class<T> clazz, DynamicClassLoader classLoader)
    {
        ClassDefinition definition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName("Single" + clazz.getSimpleName()),
                type(Object.class),
                type(clazz));

        // Store class size in static field
        FieldDefinition classSize = definition.declareField(a(PRIVATE, STATIC, FINAL), "CLASS_SIZE", long.class);
        definition.getClassInitializer()
                .getBody()
                .comment("CLASS_SIZE = ClassLayout.parseClass(%s.class).instanceSize()", definition.getName())
                .push(definition.getType())
                .invokeStatic(ClassLayout.class, "parseClass", ClassLayout.class, Class.class)
                .invokeVirtual(ClassLayout.class, "instanceSize", int.class)
                .intToLong()
                .putStaticField(classSize);

        // Add getter for class size
        definition.declareMethod(a(PUBLIC), "getEstimatedSize", type(long.class))
                .getBody()
                .getStaticField(classSize)
                .retLong();

        // Generate constructor
        MethodDefinition constructor = definition.declareConstructor(a(PUBLIC));

        constructor.getBody()
                .append(constructor.getThis())
                .invokeConstructor(Object.class);

        // Generate fields
        List<StateField> fields = enumerateFields(clazz);
        for (StateField field : fields) {
            generateField(definition, constructor, field);
        }

        constructor.getBody()
                .ret();

        return defineClass(definition, clazz, classLoader);
    }

    private static <T> Class<? extends T> generateGroupedStateClass(Class<T> clazz, DynamicClassLoader classLoader)
    {
        ClassDefinition definition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName("Grouped" + clazz.getSimpleName()),
                type(AbstractGroupedAccumulatorState.class),
                type(clazz),
                type(GroupedAccumulator.class));

        List<StateField> fields = enumerateFields(clazz);

        // Create constructor
        MethodDefinition constructor = definition.declareConstructor(a(PUBLIC));
        constructor.getBody()
                .append(constructor.getThis())
                .invokeConstructor(AbstractGroupedAccumulatorState.class);

        // Create ensureCapacity
        MethodDefinition ensureCapacity = definition.declareMethod(a(PUBLIC), "ensureCapacity", type(void.class), arg("size", long.class));

        // Generate fields, constructor, and ensureCapacity
        List<FieldDefinition> fieldDefinitions = new ArrayList<>();
        for (StateField field : fields) {
            fieldDefinitions.add(generateGroupedField(definition, constructor, ensureCapacity, field));
        }

        constructor.getBody().ret();
        ensureCapacity.getBody().ret();

        // Generate getEstimatedSize
        MethodDefinition getEstimatedSize = definition.declareMethod(a(PUBLIC), "getEstimatedSize", type(long.class));
        BytecodeBlock body = getEstimatedSize.getBody();

        Variable size = getEstimatedSize.getScope().declareVariable(long.class, "size");

        // initialize size to 0L
        body.append(size.set(constantLong(0)));

        // add field to size
        for (FieldDefinition field : fieldDefinitions) {
            body.append(size.set(add(size, getEstimatedSize.getThis().getField(field).invoke("sizeOf", long.class))));
        }

        // return size
        body.append(size.ret());

        return defineClass(definition, clazz, classLoader);
    }

    private static void generateField(ClassDefinition definition, MethodDefinition constructor, StateField stateField)
    {
        FieldDefinition field = definition.declareField(a(PRIVATE), UPPER_CAMEL.to(LOWER_CAMEL, stateField.getName()) + "Value", stateField.getType());

        // Generate getter
        MethodDefinition getter = definition.declareMethod(a(PUBLIC), stateField.getGetterName(), type(stateField.getType()));
        getter.getBody()
                .append(getter.getThis().getField(field).ret());

        // Generate setter
        Parameter value = arg("value", stateField.getType());
        MethodDefinition setter = definition.declareMethod(a(PUBLIC), stateField.getSetterName(), type(void.class), value);
        setter.getBody()
                .append(setter.getThis().setField(field, value))
                .ret();

        constructor.getBody()
                .append(constructor.getThis().setField(field, stateField.initialValueExpression()));
    }

    private static FieldDefinition generateGroupedField(ClassDefinition definition, MethodDefinition constructor, MethodDefinition ensureCapacity, StateField stateField)
    {
        Class<?> bigArrayType = getBigArrayType(stateField.getType());
        FieldDefinition field = definition.declareField(a(PRIVATE), UPPER_CAMEL.to(LOWER_CAMEL, stateField.getName()) + "Values", bigArrayType);

        // Generate getter
        MethodDefinition getter = definition.declareMethod(a(PUBLIC), stateField.getGetterName(), type(stateField.getType()));
        getter.getBody()
                .append(getter.getThis().getField(field).invoke(
                        "get",
                        stateField.getType(),
                        getter.getThis().invoke("getGroupId", long.class))
                        .ret());

        // Generate setter
        Parameter value = arg("value", stateField.getType());
        MethodDefinition setter = definition.declareMethod(a(PUBLIC), stateField.getSetterName(), type(void.class), value);
        setter.getBody()
                .append(setter.getThis().getField(field).invoke(
                        "set",
                        void.class,
                        setter.getThis().invoke("getGroupId", long.class),
                        value))
                .ret();

        Scope ensureCapacityScope = ensureCapacity.getScope();
        ensureCapacity.getBody()
                .append(ensureCapacity.getThis().getField(field).invoke("ensureCapacity", void.class, ensureCapacityScope.getVariable("size")));

        // Initialize field in constructor
        constructor.getBody()
                .append(constructor.getThis().setField(field, newInstance(field.getType(), stateField.initialValueExpression())));

        return field;
    }

    /**
     * Enumerates all the fields in this state interface.
     *
     * @param clazz a subclass of AccumulatorState
     * @return list of state fields. Ordering is guaranteed to be stable, and have all primitive fields at the beginning.
     */
    private static List<StateField> enumerateFields(Class<?> clazz)
    {
        ImmutableList.Builder<StateField> builder = ImmutableList.builder();
        final Set<Class<?>> primitiveClasses = ImmutableSet.of(byte.class, boolean.class, long.class, double.class);
        Set<Class<?>> supportedClasses = ImmutableSet.of(byte.class, boolean.class, long.class, double.class, Slice.class, Block.class);

        for (Method method : clazz.getMethods()) {
            if (method.getName().equals("getEstimatedSize")) {
                continue;
            }
            if (method.getName().startsWith("get")) {
                Class<?> type = method.getReturnType();
                checkArgument(supportedClasses.contains(type), type.getName() + " is not supported");
                String name = method.getName().substring(3);
                builder.add(new StateField(name, type, getInitialValue(method)));
            }
            if (method.getName().startsWith("is")) {
                Class<?> type = method.getReturnType();
                checkArgument(type == boolean.class, "Only boolean is support for 'is' methods");
                String name = method.getName().substring(2);
                builder.add(new StateField(name, type, getInitialValue(method), method.getName()));
            }
        }

        // We need this ordering because the serializer and deserializer are on different machines, and so the ordering of fields must be stable
        Ordering<StateField> ordering = new Ordering<StateField>()
        {
            @Override
            public int compare(StateField left, StateField right)
            {
                if (primitiveClasses.contains(left.getType()) && !primitiveClasses.contains(right.getType())) {
                    return -1;
                }
                if (primitiveClasses.contains(right.getType()) && !primitiveClasses.contains(left.getType())) {
                    return 1;
                }
                // If they're the category, just sort by name
                return left.getName().compareTo(right.getName());
            }
        };
        List<StateField> fields = ordering.sortedCopy(builder.build());
        checkInterface(clazz, fields);

        return fields;
    }

    private static Object getInitialValue(Method method)
    {
        Object value = null;

        for (Annotation annotation : method.getAnnotations()) {
            if (annotation instanceof InitialLongValue) {
                checkArgument(value == null, "%s has multiple initialValue annotations", method.getName());
                checkArgument(method.getReturnType() == long.class, "%s does not return a long, but is annotated with @InitialLongValue", method.getName());
                value = ((InitialLongValue) annotation).value();
            }
            else if (annotation instanceof InitialDoubleValue) {
                checkArgument(value == null, "%s has multiple initialValue annotations", method.getName());
                checkArgument(method.getReturnType() == double.class, "%s does not return a double, but is annotated with @InitialDoubleValue", method.getName());
                value = ((InitialDoubleValue) annotation).value();
            }
            else if (annotation instanceof InitialBooleanValue) {
                checkArgument(value == null, "%s has multiple initialValue annotations", method.getName());
                checkArgument(method.getReturnType() == boolean.class, "%s does not return a boolean, but is annotated with @InitialBooleanValue", method.getName());
                value = ((InitialBooleanValue) annotation).value();
            }
        }

        return value;
    }

    private static void checkInterface(Class<?> clazz, List<StateField> fields)
    {
        checkArgument(clazz.isInterface(), clazz.getName() + " is not an interface");
        Set<String> setters = new HashSet<>();
        Set<String> getters = new HashSet<>();
        Set<String> isGetters = new HashSet<>();

        Map<String, Class<?>> fieldTypes = new HashMap<>();
        for (StateField field : fields) {
            fieldTypes.put(field.getName(), field.getType());
        }

        for (Method method : clazz.getMethods()) {
            if (Modifier.isStatic(method.getModifiers())) {
                continue;
            }

            if (method.getName().equals("getEstimatedSize")) {
                checkArgument(method.getReturnType().equals(long.class), "getEstimatedSize must return long");
                checkArgument(method.getParameterTypes().length == 0, "getEstimatedSize may not have parameters");
                continue;
            }

            if (method.getName().startsWith("get")) {
                String name = method.getName().substring(3);
                checkArgument(fieldTypes.get(name).equals(method.getReturnType()),
                        "Expected %s to return type %s, but found %s", method.getName(), fieldTypes.get(name), method.getReturnType());
                checkArgument(method.getParameterTypes().length == 0, "Expected %s to have zero parameters", method.getName());
                getters.add(name);
            }
            else if (method.getName().startsWith("is")) {
                String name = method.getName().substring(2);
                checkArgument(fieldTypes.get(name) == boolean.class,
                        "Expected %s to have type boolean, but found %s", name, fieldTypes.get(name));
                checkArgument(method.getParameterTypes().length == 0, "Expected %s to have zero parameters", method.getName());
                checkArgument(method.getReturnType() == boolean.class, "Expected %s to return boolean", method.getName());
                isGetters.add(name);
            }
            else if (method.getName().startsWith("set")) {
                String name = method.getName().substring(3);
                checkArgument(method.getParameterTypes().length == 1, "Expected setter to have one parameter");
                checkArgument(fieldTypes.get(name).equals(method.getParameterTypes()[0]),
                        "Expected %s to accept type %s, but found %s", method.getName(), fieldTypes.get(name), method.getParameterTypes()[0]);
                checkArgument(getInitialValue(method) == null, "initial value annotation not allowed on setter");
                checkArgument(method.getReturnType().equals(void.class), "%s may not return a value", method.getName());
                setters.add(name);
            }
            else {
                throw new IllegalArgumentException("Cannot generate implementation for method: " + method.getName());
            }
        }
        checkArgument(getters.size() + isGetters.size() == setters.size() && setters.size() == fields.size(), "Wrong number of getters/setters");
    }

    private static final class StateField
    {
        private final String name;
        private final String getterName;
        private final Class<?> type;
        private final Object initialValue;

        private StateField(String name, Class<?> type, Object initialValue)
        {
            this(name, type, initialValue, "get" + name);
        }

        private StateField(String name, Class<?> type, Object initialValue, String getterName)
        {
            this.name = requireNonNull(name, "name is null");
            checkArgument(!name.isEmpty(), "name is empty");
            this.type = requireNonNull(type, "type is null");
            this.getterName = requireNonNull(getterName, "getterName is null");
            this.initialValue = initialValue;
        }

        public String getGetterName()
        {
            return getterName;
        }

        public String getSetterName()
        {
            return "set" + getName();
        }

        public String getName()
        {
            return name;
        }

        public Class<?> getType()
        {
            return type;
        }

        public int sizeOfType()
        {
            if (getType() == long.class) {
                return SizeOf.SIZE_OF_LONG;
            }
            else if (getType() == double.class) {
                return SizeOf.SIZE_OF_DOUBLE;
            }
            else if (getType() == boolean.class || getType() == byte.class) {
                return SizeOf.SIZE_OF_BYTE;
            }
            else {
                throw new IllegalArgumentException("Unsupported type: " + getType());
            }
        }

        public Object getInitialValue()
        {
            return initialValue;
        }

        public BytecodeExpression initialValueExpression()
        {
            if (initialValue == null) {
                return defaultValue(type);
            }
            if (initialValue instanceof Number) {
                return constantNumber((Number) initialValue);
            }
            else if (initialValue instanceof Boolean) {
                return constantBoolean((boolean) initialValue);
            }
            else {
                throw new IllegalArgumentException("Unsupported initial value type: " + initialValue.getClass());
            }
        }
    }
}
