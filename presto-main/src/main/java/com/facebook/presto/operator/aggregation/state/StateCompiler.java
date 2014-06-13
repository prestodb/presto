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

import com.facebook.presto.byteCode.Block;
import com.facebook.presto.byteCode.ClassDefinition;
import com.facebook.presto.byteCode.ClassInfoLoader;
import com.facebook.presto.byteCode.CompilerContext;
import com.facebook.presto.byteCode.DumpByteCodeVisitor;
import com.facebook.presto.byteCode.DynamicClassLoader;
import com.facebook.presto.byteCode.FieldDefinition;
import com.facebook.presto.byteCode.LocalVariableDefinition;
import com.facebook.presto.byteCode.SmartClassWriter;
import com.facebook.presto.operator.aggregation.GroupedAccumulator;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.util.array.BooleanBigArray;
import com.facebook.presto.util.array.ByteBigArray;
import com.facebook.presto.util.array.DoubleBigArray;
import com.facebook.presto.util.array.LongBigArray;
import com.facebook.presto.util.array.SliceBigArray;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Ordering;
import io.airlift.slice.SizeOf;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.objectweb.asm.ClassWriter;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.presto.byteCode.Access.FINAL;
import static com.facebook.presto.byteCode.Access.PRIVATE;
import static com.facebook.presto.byteCode.Access.PUBLIC;
import static com.facebook.presto.byteCode.Access.a;
import static com.facebook.presto.byteCode.NamedParameterDefinition.arg;
import static com.facebook.presto.byteCode.ParameterizedType.type;
import static com.facebook.presto.byteCode.ParameterizedType.typeFromPathName;
import static com.google.common.base.CaseFormat.LOWER_CAMEL;
import static com.google.common.base.CaseFormat.UPPER_CAMEL;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class StateCompiler
{
    private static final boolean DUMP_BYTE_CODE_TREE = false;

    private static final AtomicLong CLASS_ID = new AtomicLong();

    private DynamicClassLoader createClassLoader()
    {
        return new DynamicClassLoader(getClass().getClassLoader());
    }

    private static Map<String, Class<?>> defineClasses(List<ClassDefinition> classDefinitions, DynamicClassLoader classLoader)
    {
        ClassInfoLoader classInfoLoader = ClassInfoLoader.createClassInfoLoader(classDefinitions, classLoader);

        if (DUMP_BYTE_CODE_TREE) {
            DumpByteCodeVisitor dumpByteCode = new DumpByteCodeVisitor(System.out);
            for (ClassDefinition classDefinition : classDefinitions) {
                dumpByteCode.visitClass(classDefinition);
            }
        }

        Map<String, byte[]> byteCodes = new LinkedHashMap<>();
        for (ClassDefinition classDefinition : classDefinitions) {
            ClassWriter cw = new SmartClassWriter(classInfoLoader);
            classDefinition.visit(cw);
            byte[] byteCode = cw.toByteArray();
            byteCodes.put(classDefinition.getType().getJavaClassName(), byteCode);
        }

        return classLoader.defineClasses(byteCodes);
    }

    private static <T> Class<? extends T> defineClass(ClassDefinition classDefinition, Class<T> superType, DynamicClassLoader classLoader)
    {
        Class<?> clazz = defineClasses(ImmutableList.of(classDefinition), classLoader).values().iterator().next();
        return clazz.asSubclass(superType);
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
        // TODO: support more reference types
        throw new IllegalArgumentException("Unsupported type: " + type.getName());
    }

    public <T> AccumulatorStateSerializer<T> generateStateSerializer(Class<T> clazz)
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

        DynamicClassLoader classLoader = createClassLoader();

        ClassDefinition definition = new ClassDefinition(new CompilerContext(null),
                a(PUBLIC, FINAL),
                typeFromPathName(clazz.getSimpleName() + "Serializer_" + CLASS_ID.incrementAndGet()),
                type(Object.class),
                type(AccumulatorStateSerializer.class));

        // Generate constructor
        definition.declareConstructor(new CompilerContext(null), a(PUBLIC))
                .getBody()
                .pushThis()
                .invokeConstructor(Object.class)
                .ret();

        List<StateField> fields = enumerateFields(clazz);
        generateSerialize(definition, clazz, fields);
        generateDeserialize(definition, clazz, fields);

        Class<? extends AccumulatorStateSerializer> serializerClass = defineClass(definition, AccumulatorStateSerializer.class, classLoader);
        try {
            return (AccumulatorStateSerializer<T>) serializerClass.newInstance();
        }
        catch (InstantiationException | IllegalAccessException e) {
            throw Throwables.propagate(e);
        }
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
        CompilerContext compilerContext = new CompilerContext(null);
        Block deserializerBody = definition.declareMethod(compilerContext, a(PUBLIC), "deserialize", type(void.class), arg("block", com.facebook.presto.spi.block.Block.class), arg("index", int.class), arg("state", Object.class)).getBody();

        if (fields.size() == 1) {
            generatePrimitiveDeserializer(deserializerBody, getSetter(clazz, fields.get(0)));
        }
        else {
            LocalVariableDefinition slice = compilerContext.declareVariable(Slice.class, "slice");
            deserializerBody.comment("Slice slice = block.getSlice(index);")
                    .getVariable("block")
                    .getVariable("index")
                    .invokeInterface(com.facebook.presto.spi.block.Block.class, "getSlice", Slice.class, int.class)
                    .putVariable(slice);

            for (StateField field : fields) {
                generateDeserializeFromSlice(deserializerBody, slice, getSetter(clazz, field), offsetOfField(field, fields));
            }
        }
        deserializerBody.ret();
    }

    private static <T> void generateSerialize(ClassDefinition definition, Class<T> clazz, List<StateField> fields)
    {
        CompilerContext compilerContext = new CompilerContext(null);
        Block serializerBody = definition.declareMethod(compilerContext, a(PUBLIC), "serialize", type(void.class), arg("state", Object.class), arg("out", BlockBuilder.class)).getBody();

        if (fields.size() == 1) {
            generatePrimitiveSerializer(serializerBody, getGetter(clazz, fields.get(0)));
        }
        else {
            LocalVariableDefinition slice = compilerContext.declareVariable(Slice.class, "slice");
            int size = serializedSizeOf(clazz);
            serializerBody.comment("Slice slice = Slices.allocate(%d);", size)
                    .push(size)
                    .invokeStatic(Slices.class, "allocate", Slice.class, int.class)
                    .putVariable(slice);

            for (StateField field : fields) {
                generateSerializeFieldToSlice(serializerBody, slice, getGetter(clazz, field), offsetOfField(field, fields));
            }
            serializerBody.comment("out.appendSlice(slice);")
                    .getVariable("out")
                    .getVariable(slice)
                    .invokeInterface(BlockBuilder.class, "appendSlice", BlockBuilder.class, Slice.class);
        }
        serializerBody.ret();
    }

    private static void generateSerializeFieldToSlice(Block body, LocalVariableDefinition slice, Method getter, int offset)
    {
        Method sliceSetterMethod = StateCompilerUtils.getSliceSetter(getter.getReturnType());
        body.comment("slice.%s(offset, state.%s())", sliceSetterMethod.getName(), getter.getName())
                .getVariable(slice)
                .push(offset)
                .getVariable("state")
                .invokeInterface(getter)
                .invokeStatic(sliceSetterMethod);
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

    private static void generatePrimitiveSerializer(Block body, Method getter)
    {
        Method method = StateCompilerUtils.getBlockBuilderAppend(getter.getReturnType());
        body.comment("out.%s(state.%s());", method.getName(), getter.getName())
                .getVariable("out")
                .getVariable("state")
                .invokeInterface(getter)
                .invokeStatic(method);
    }

    private static void generatePrimitiveDeserializer(Block body, Method setter)
    {
        Method method = StateCompilerUtils.getBlockGetter(setter.getParameterTypes()[0]);
        body.comment("state.%s(block.%s));", setter.getName(), method.getName())
                .getVariable("state")
                .getVariable("block")
                .getVariable("index")
                .invokeStatic(method)
                .invokeInterface(setter);
    }

    private static void generateDeserializeFromSlice(Block body, LocalVariableDefinition slice, Method setter, int offset)
    {
        Method sliceGetterMethod = StateCompilerUtils.getSliceGetter(setter.getParameterTypes()[0]);
        body.comment("state.%s(slice.%s(%d))", setter.getName(), sliceGetterMethod.getName(), offset)
                .getVariable("state")
                .getVariable(slice)
                .push(offset)
                .invokeStatic(sliceGetterMethod)
                .invokeInterface(setter);
    }

    public <T> AccumulatorStateFactory<T> generateStateFactory(Class<T> clazz)
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

        DynamicClassLoader classLoader = createClassLoader();
        Class<? extends T> singleStateClass = generateSingleStateClass(clazz, classLoader);
        Class<? extends T> groupedStateClass = generateGroupedStateClass(clazz, classLoader);

        ClassDefinition definition = new ClassDefinition(new CompilerContext(null),
                a(PUBLIC, FINAL),
                typeFromPathName(clazz.getSimpleName() + "Factory_" + CLASS_ID.incrementAndGet()),
                type(Object.class),
                type(AccumulatorStateFactory.class));

        // Generate constructor
        definition.declareConstructor(new CompilerContext(null), a(PUBLIC))
                .getBody()
                .pushThis()
                .invokeConstructor(Object.class)
                .ret();

        // Generate single state creation method
        definition.declareMethod(new CompilerContext(null), a(PUBLIC), "createSingleState", type(Object.class))
                .getBody()
                .newObject(singleStateClass)
                .dup()
                .invokeConstructor(singleStateClass)
                .retObject();

        // Generate grouped state creation method
        definition.declareMethod(new CompilerContext(null), a(PUBLIC), "createGroupedState", type(Object.class))
                .getBody()
                .newObject(groupedStateClass)
                .dup()
                .invokeConstructor(groupedStateClass)
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
        ClassDefinition definition = new ClassDefinition(new CompilerContext(null),
                a(PUBLIC, FINAL),
                typeFromPathName("Single" + clazz.getSimpleName() + "_" + CLASS_ID.incrementAndGet()),
                type(AbstractAccumulatorState.class),
                type(clazz));

        // Generate constructor
        Block constructor = definition.declareConstructor(new CompilerContext(null), a(PUBLIC))
                .getBody()
                .pushThis()
                .invokeConstructor(AbstractAccumulatorState.class);

        // Generate fields
        List<StateField> fields = enumerateFields(clazz);
        for (StateField field : fields) {
            generateField(definition, constructor, field);
        }

        constructor.ret();

        return defineClass(definition, clazz, classLoader);
    }

    private static <T> Class<? extends T> generateGroupedStateClass(Class<T> clazz, DynamicClassLoader classLoader)
    {
        ClassDefinition definition = new ClassDefinition(new CompilerContext(null),
                a(PUBLIC, FINAL),
                typeFromPathName("Grouped" + clazz.getSimpleName() + "_" + CLASS_ID.incrementAndGet()),
                type(AbstractGroupedAccumulatorState.class),
                type(clazz),
                type(GroupedAccumulator.class));

        List<StateField> fields = enumerateFields(clazz);

        // Create constructor
        Block constructor = definition.declareConstructor(new CompilerContext(null), a(PUBLIC))
                .getBody()
                .pushThis()
                .invokeConstructor(AbstractGroupedAccumulatorState.class);
        // Create ensureCapacity
        Block ensureCapacity = definition.declareMethod(new CompilerContext(null), a(PUBLIC), "ensureCapacity", type(void.class), arg("size", long.class)).getBody();

        // Generate fields, constructor, and ensureCapacity
        List<FieldDefinition> fieldDefinitions = new ArrayList<>();
        for (StateField field : fields) {
            fieldDefinitions.add(generateGroupedField(definition, constructor, ensureCapacity, field));
        }

        constructor.ret();
        ensureCapacity.ret();

        // Generate getEstimatedSize
        Block getEstimatedSize = definition.declareMethod(new CompilerContext(null), a(PUBLIC), "getEstimatedSize", type(long.class))
                .getBody()
                .comment("long size = 0;")
                .push(0L);
        for (FieldDefinition field : fieldDefinitions) {
            getEstimatedSize
                    .comment("size += %s.sizeOf();", field.getName())
                    .pushThis()
                    .getField(field)
                    .invokeVirtual(field.getType(), "sizeOf", type(long.class))
                    .longAdd();
        }
        getEstimatedSize.comment("return size;");
        getEstimatedSize.retLong();

        return defineClass(definition, clazz, classLoader);
    }

    private static void generateField(ClassDefinition definition, Block constructor, StateField stateField)
    {
        FieldDefinition field = definition.declareField(a(PRIVATE), UPPER_CAMEL.to(LOWER_CAMEL, stateField.getName()) + "Value", stateField.getType());

        // Generate getter
        definition.declareMethod(new CompilerContext(null), a(PUBLIC), stateField.getGetterName(), type(stateField.getType()))
                .getBody()
                .pushThis()
                .getField(field)
                .ret(stateField.getType());

        // Generate setter
        definition.declareMethod(new CompilerContext(null), a(PUBLIC), stateField.getSetterName(), type(void.class), arg("value", stateField.getType()))
                .getBody()
                .pushThis()
                .getVariable("value")
                .putField(field)
                .ret();

        constructor.pushThis();
        pushInitialValue(constructor, stateField);
        constructor.putField(field);
    }

    private static FieldDefinition generateGroupedField(ClassDefinition definition, Block constructor, Block ensureCapacity, StateField stateField)
    {
        Class<?> bigArrayType = getBigArrayType(stateField.getType());
        FieldDefinition field = definition.declareField(a(PRIVATE), UPPER_CAMEL.to(LOWER_CAMEL, stateField.getName()) + "Values", bigArrayType);

        // Generate getter
        definition.declareMethod(new CompilerContext(null), a(PUBLIC), stateField.getGetterName(), type(stateField.getType()))
                .getBody()
                .comment("return field.get(getGroupId());")
                .pushThis()
                .getField(field)
                .pushThis()
                .invokeVirtual(AbstractGroupedAccumulatorState.class, "getGroupId", long.class)
                .invokeVirtual(bigArrayType, "get", stateField.getType(), long.class)
                .ret(stateField.getType());

        // Generate setter
        definition.declareMethod(new CompilerContext(null), a(PUBLIC), stateField.getSetterName(), type(void.class), arg("value", stateField.getType()))
                .getBody()
                .comment("return field.set(getGroupId(), value);")
                .pushThis()
                .getField(field)
                .pushThis()
                .invokeVirtual(AbstractGroupedAccumulatorState.class, "getGroupId", long.class)
                .getVariable("value")
                .invokeVirtual(bigArrayType, "set", void.class, long.class, stateField.getType())
                .ret();

        ensureCapacity.pushThis()
                .getField(field)
                .getVariable("size")
                .invokeVirtual(field.getType(), "ensureCapacity", type(void.class), type(long.class));

        // Initialize field in constructor
        constructor.pushThis()
                .newObject(field.getType())
                .dup();
        pushInitialValue(constructor, stateField);
        constructor.invokeConstructor(field.getType(), type(stateField.getType()));
        constructor.putField(field);

        return field;
    }

    private static void pushInitialValue(Block block, StateField stateField)
    {
        Object initialValue = stateField.getInitialValue();
        if (initialValue != null) {
            if (initialValue instanceof Number) {
                block.push((Number) initialValue);
            }
            else if (initialValue instanceof Boolean) {
                block.push((boolean) initialValue);
            }
            else {
                throw new IllegalArgumentException("Unsupported initial value type: " + initialValue.getClass());
            }
        }
        else {
            block.pushJavaDefault(stateField.getType());
        }
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
        final Set<Class<?>> primitiveClasses = ImmutableSet.<Class<?>>of(byte.class, boolean.class, long.class, double.class);
        Set<Class<?>> supportedClasses = ImmutableSet.<Class<?>>of(byte.class, boolean.class, long.class, double.class, Slice.class);

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
            this.name = checkNotNull(name, "name is null");
            checkArgument(!name.isEmpty(), "name is empty");
            this.type = checkNotNull(type, "type is null");
            this.getterName = checkNotNull(getterName, "getterName is null");
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
    }
}
