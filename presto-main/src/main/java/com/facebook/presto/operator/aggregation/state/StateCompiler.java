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
import com.facebook.presto.byteCode.NamedParameterDefinition;
import com.facebook.presto.byteCode.SmartClassWriter;
import com.facebook.presto.operator.aggregation.GroupedAccumulator;
import com.facebook.presto.util.array.BooleanBigArray;
import com.facebook.presto.util.array.ByteBigArray;
import com.facebook.presto.util.array.DoubleBigArray;
import com.facebook.presto.util.array.LongBigArray;
import com.facebook.presto.util.array.SliceBigArray;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import org.objectweb.asm.ClassWriter;

import java.lang.annotation.Annotation;
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

    public <T> AccumulatorStateFactory<T> generateStateFactory(Class<T> clazz)
    {
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
        checkInterface(clazz, fields);
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
        checkInterface(clazz, fields);

        // Create constructor
        Block constructor = definition.declareConstructor(new CompilerContext(null), a(PUBLIC))
                .getBody()
                .pushThis()
                .invokeConstructor(AbstractGroupedAccumulatorState.class);
        // Create ensureCapacity
        Block ensureCapacity = definition.declareMethod(new CompilerContext(null), a(PUBLIC), "ensureCapacity", type(void.class), NamedParameterDefinition.arg("size", long.class)).getBody();

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
        definition.declareMethod(new CompilerContext(null), a(PUBLIC), stateField.getSetterName(), type(void.class), NamedParameterDefinition.arg("value", stateField.getType()))
                .getBody()
                .pushThis()
                .getVariable("value")
                .putField(field)
                .ret();

        if (stateField.getInitialValue() != null) {
            constructor.pushThis()
                    .push(stateField.getInitialValue())
                    .putField(field);
        }
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
        definition.declareMethod(new CompilerContext(null), a(PUBLIC), stateField.getSetterName(), type(void.class), NamedParameterDefinition.arg("value", stateField.getType()))
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
        if (stateField.getInitialValue() != null) {
            constructor.push(stateField.getInitialValue());
        }
        else {
            constructor.pushJavaDefault(stateField.getType());
        }
        constructor.invokeConstructor(field.getType(), type(stateField.getType()));
        constructor.putField(field);

        return field;
    }

    private static List<StateField> enumerateFields(Class<?> clazz)
    {
        ImmutableList.Builder<StateField> builder = ImmutableList.builder();
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
        }

        return builder.build();
    }

    private static Number getInitialValue(Method method)
    {
        Number value = null;

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
        }

        return value;
    }

    private static void checkInterface(Class<?> clazz, List<StateField> fields)
    {
        checkArgument(clazz.isInterface(), clazz.getName() + " is not an interface");
        Set<String> setters = new HashSet<>();
        Set<String> getters = new HashSet<>();

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
        checkArgument(getters.size() == setters.size() && setters.size() == fields.size(), "Wrong number of getters/setters");
    }

    private static final class StateField
    {
        private final String name;
        private final Class<?> type;
        private final Number initialValue;

        private StateField(String name, Class<?> type, Number initialValue)
        {
            this.name = checkNotNull(name, "name is null");
            checkArgument(!name.isEmpty(), "name is empty");
            this.type = checkNotNull(type, "type is null");
            this.initialValue = initialValue;
        }

        public String getGetterName()
        {
            return "get" + getName();
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

        public Number getInitialValue()
        {
            return initialValue;
        }
    }
}
