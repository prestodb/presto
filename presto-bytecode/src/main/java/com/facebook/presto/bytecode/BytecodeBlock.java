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
package com.facebook.presto.bytecode;

import com.facebook.presto.bytecode.debug.LineNumberNode;
import com.facebook.presto.bytecode.instruction.Constant;
import com.facebook.presto.bytecode.instruction.InvokeInstruction;
import com.facebook.presto.bytecode.instruction.JumpInstruction;
import com.facebook.presto.bytecode.instruction.LabelNode;
import com.facebook.presto.bytecode.instruction.TypeInstruction;
import com.facebook.presto.bytecode.instruction.VariableInstruction;
import com.google.common.collect.ImmutableList;
import org.objectweb.asm.MethodVisitor;

import javax.annotation.concurrent.NotThreadSafe;

import java.lang.invoke.MethodType;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.bytecode.Access.STATIC;
import static com.facebook.presto.bytecode.ParameterizedType.type;
import static com.facebook.presto.bytecode.instruction.Constant.loadBoolean;
import static com.facebook.presto.bytecode.instruction.Constant.loadClass;
import static com.facebook.presto.bytecode.instruction.Constant.loadDouble;
import static com.facebook.presto.bytecode.instruction.Constant.loadFloat;
import static com.facebook.presto.bytecode.instruction.Constant.loadInt;
import static com.facebook.presto.bytecode.instruction.Constant.loadLong;
import static com.facebook.presto.bytecode.instruction.Constant.loadNumber;
import static com.facebook.presto.bytecode.instruction.FieldInstruction.getFieldInstruction;
import static com.facebook.presto.bytecode.instruction.FieldInstruction.getStaticInstruction;
import static com.facebook.presto.bytecode.instruction.FieldInstruction.putFieldInstruction;
import static com.facebook.presto.bytecode.instruction.FieldInstruction.putStaticInstruction;
import static com.facebook.presto.bytecode.instruction.TypeInstruction.cast;
import static com.facebook.presto.bytecode.instruction.TypeInstruction.instanceOf;
import static com.facebook.presto.bytecode.instruction.VariableInstruction.loadVariable;
import static com.facebook.presto.bytecode.instruction.VariableInstruction.storeVariable;
import static com.google.common.base.Preconditions.checkArgument;

@SuppressWarnings("UnusedDeclaration")
@NotThreadSafe
public class BytecodeBlock
        implements BytecodeNode
{
    private final List<BytecodeNode> nodes = new ArrayList<>();

    private String description;
    private int currentLineNumber = -1;

    public String getDescription()
    {
        return description;
    }

    public BytecodeBlock setDescription(String description)
    {
        this.description = description;
        return this;
    }

    @Override
    public List<BytecodeNode> getChildNodes()
    {
        return ImmutableList.copyOf(nodes);
    }

    public BytecodeBlock append(BytecodeNode node)
    {
        nodes.add(node);
        return this;
    }

    public BytecodeBlock comment(String comment)
    {
        nodes.add(new Comment(comment));
        return this;
    }

    public BytecodeBlock comment(String comment, Object... args)
    {
        nodes.add(new Comment(String.format(comment, args)));
        return this;
    }

    public boolean isEmpty()
    {
        return nodes.isEmpty();
    }

    public BytecodeBlock visitLabel(LabelNode label)
    {
        nodes.add(label);
        return this;
    }

    public BytecodeBlock gotoLabel(LabelNode label)
    {
        nodes.add(JumpInstruction.jump(label));
        return this;
    }

    public BytecodeBlock ifFalseGoto(LabelNode label)
    {
        return ifZeroGoto(label);
    }

    public BytecodeBlock ifTrueGoto(LabelNode label)
    {
        return ifNotZeroGoto(label);
    }

    public BytecodeBlock ifZeroGoto(LabelNode label)
    {
        nodes.add(JumpInstruction.jumpIfEqualZero(label));
        return this;
    }

    public BytecodeBlock ifNotZeroGoto(LabelNode label)
    {
        nodes.add(JumpInstruction.jumpIfNotEqualZero(label));
        return this;
    }

    public BytecodeBlock ifNullGoto(LabelNode label)
    {
        nodes.add(JumpInstruction.jumpIfNull(label));
        return this;
    }

    public BytecodeBlock ifNotNullGoto(LabelNode label)
    {
        nodes.add(JumpInstruction.jumpIfNotNull(label));
        return this;
    }

    public BytecodeBlock intAdd()
    {
        nodes.add(OpCode.IADD);
        return this;
    }

    public BytecodeBlock longAdd()
    {
        nodes.add(OpCode.LADD);
        return this;
    }

    public BytecodeBlock longCompare()
    {
        nodes.add(OpCode.LCMP);
        return this;
    }

    /**
     * Compare two doubles. If either is NaN comparison is -1.
     */
    public BytecodeBlock doubleCompareNanLess()
    {
        nodes.add(OpCode.DCMPL);
        return this;
    }

    /**
     * Compare two doubles. If either is NaN comparison is 1.
     */
    public BytecodeBlock doubleCompareNanGreater()
    {
        nodes.add(OpCode.DCMPG);
        return this;
    }

    public BytecodeBlock intLeftShift()
    {
        nodes.add(OpCode.ISHL);
        return this;
    }

    public BytecodeBlock intRightShift()
    {
        nodes.add(OpCode.ISHR);
        return this;
    }

    public BytecodeBlock longLeftShift()
    {
        nodes.add(OpCode.LSHL);
        return this;
    }

    public BytecodeBlock longRightShift()
    {
        nodes.add(OpCode.LSHR);
        return this;
    }

    public BytecodeBlock unsignedIntRightShift()
    {
        nodes.add(OpCode.IUSHR);
        return this;
    }

    public BytecodeBlock unsignedLongRightShift()
    {
        nodes.add(OpCode.LUSHR);
        return this;
    }

    public BytecodeBlock intBitAnd()
    {
        nodes.add(OpCode.IAND);
        return this;
    }

    public BytecodeBlock intBitOr()
    {
        nodes.add(OpCode.IOR);
        return this;
    }

    public BytecodeBlock intBitXor()
    {
        nodes.add(OpCode.IXOR);
        return this;
    }

    public BytecodeBlock longBitAnd()
    {
        nodes.add(OpCode.LAND);
        return this;
    }

    public BytecodeBlock longBitOr()
    {
        nodes.add(OpCode.LOR);
        return this;
    }

    public BytecodeBlock longBitXor()
    {
        nodes.add(OpCode.LXOR);
        return this;
    }

    public BytecodeBlock intNegate()
    {
        nodes.add(OpCode.INEG);
        return this;
    }

    public BytecodeBlock intToLong()
    {
        nodes.add(OpCode.I2L);
        return this;
    }

    public BytecodeBlock longNegate()
    {
        nodes.add(OpCode.LNEG);
        return this;
    }

    public BytecodeBlock longToInt()
    {
        nodes.add(OpCode.L2I);
        return this;
    }

    public BytecodeBlock isInstanceOf(Class<?> type)
    {
        nodes.add(instanceOf(type));
        return this;
    }

    public BytecodeBlock isInstanceOf(ParameterizedType type)
    {
        nodes.add(instanceOf(type));
        return this;
    }

    public BytecodeBlock checkCast(Class<?> type)
    {
        nodes.add(cast(type));
        return this;
    }

    public BytecodeBlock checkCast(ParameterizedType type)
    {
        nodes.add(cast(type));
        return this;
    }

    public BytecodeBlock invokeStatic(Method method)
    {
        nodes.add(InvokeInstruction.invokeStatic(method));
        return this;
    }

    public BytecodeBlock invokeStatic(MethodDefinition method)
    {
        nodes.add(InvokeInstruction.invokeStatic(method));
        return this;
    }

    public BytecodeBlock invokeStatic(Class<?> type, String name, Class<?> returnType, Class<?>... parameterTypes)
    {
        nodes.add(InvokeInstruction.invokeStatic(type, name, returnType, parameterTypes));
        return this;
    }

    public BytecodeBlock invokeStatic(Class<?> type, String name, Class<?> returnType, Iterable<Class<?>> parameterTypes)
    {
        nodes.add(InvokeInstruction.invokeStatic(type, name, returnType, parameterTypes));
        return this;
    }

    public BytecodeBlock invokeStatic(ParameterizedType type, String name, ParameterizedType returnType, ParameterizedType... parameterTypes)
    {
        nodes.add(InvokeInstruction.invokeStatic(type, name, returnType, parameterTypes));
        return this;
    }

    public BytecodeBlock invokeStatic(ParameterizedType type, String name, ParameterizedType returnType, Iterable<ParameterizedType> parameterTypes)
    {
        nodes.add(InvokeInstruction.invokeStatic(type, name, returnType, parameterTypes));
        return this;
    }

    public BytecodeBlock invokeVirtual(Method method)
    {
        nodes.add(InvokeInstruction.invokeVirtual(method));
        return this;
    }

    public BytecodeBlock invokeVirtual(MethodDefinition method)
    {
        nodes.add(InvokeInstruction.invokeVirtual(method));
        return this;
    }

    public BytecodeBlock invokeVirtual(Class<?> type, String name, Class<?> returnType, Class<?>... parameterTypes)
    {
        nodes.add(InvokeInstruction.invokeVirtual(type, name, returnType, parameterTypes));
        return this;
    }

    public BytecodeBlock invokeVirtual(Class<?> type, String name, Class<?> returnType, Iterable<Class<?>> parameterTypes)
    {
        nodes.add(InvokeInstruction.invokeVirtual(type, name, returnType, parameterTypes));
        return this;
    }

    public BytecodeBlock invokeVirtual(ParameterizedType type, String name, ParameterizedType returnType, ParameterizedType... parameterTypes)
    {
        nodes.add(InvokeInstruction.invokeVirtual(type, name, returnType, parameterTypes));
        return this;
    }

    public BytecodeBlock invokeVirtual(ParameterizedType type, String name, ParameterizedType returnType, Iterable<ParameterizedType> parameterTypes)
    {
        nodes.add(InvokeInstruction.invokeVirtual(type, name, returnType, parameterTypes));
        return this;
    }

    public BytecodeBlock invokeInterface(Method method)
    {
        nodes.add(InvokeInstruction.invokeInterface(method));
        return this;
    }

    public BytecodeBlock invokeInterface(MethodDefinition method)
    {
        nodes.add(InvokeInstruction.invokeInterface(method));
        return this;
    }

    public BytecodeBlock invokeInterface(Class<?> type, String name, Class<?> returnType, Class<?>... parameterTypes)
    {
        nodes.add(InvokeInstruction.invokeInterface(type, name, returnType, parameterTypes));
        return this;
    }

    public BytecodeBlock invokeInterface(Class<?> type, String name, Class<?> returnType, Iterable<Class<?>> parameterTypes)
    {
        nodes.add(InvokeInstruction.invokeInterface(type, name, returnType, parameterTypes));
        return this;
    }

    public BytecodeBlock invokeInterface(ParameterizedType type, String name, ParameterizedType returnType, ParameterizedType... parameterTypes)
    {
        nodes.add(InvokeInstruction.invokeInterface(type, name, returnType, parameterTypes));
        return this;
    }

    public BytecodeBlock invokeInterface(ParameterizedType type, String name, ParameterizedType returnType, Iterable<ParameterizedType> parameterTypes)
    {
        nodes.add(InvokeInstruction.invokeInterface(type, name, returnType, parameterTypes));
        return this;
    }

    public BytecodeBlock invokeConstructor(Constructor<?> constructor)
    {
        nodes.add(InvokeInstruction.invokeConstructor(constructor));
        return this;
    }

    public BytecodeBlock invokeConstructor(Class<?> type, Class<?>... parameterTypes)
    {
        nodes.add(InvokeInstruction.invokeConstructor(type, parameterTypes));
        return this;
    }

    public BytecodeBlock invokeConstructor(Class<?> type, Iterable<Class<?>> parameterTypes)
    {
        nodes.add(InvokeInstruction.invokeConstructor(type, parameterTypes));
        return this;
    }

    public BytecodeBlock invokeConstructor(ParameterizedType type, ParameterizedType... parameterTypes)
    {
        nodes.add(InvokeInstruction.invokeConstructor(type, parameterTypes));
        return this;
    }

    public BytecodeBlock invokeConstructor(ParameterizedType type, Iterable<ParameterizedType> parameterTypes)
    {
        nodes.add(InvokeInstruction.invokeConstructor(type, parameterTypes));
        return this;
    }

    public BytecodeBlock invokeSpecial(Method method)
    {
        nodes.add(InvokeInstruction.invokeSpecial(method));
        return this;
    }

    public BytecodeBlock invokeSpecial(MethodDefinition method)
    {
        nodes.add(InvokeInstruction.invokeSpecial(method));
        return this;
    }

    public BytecodeBlock invokeSpecial(Class<?> type, String name, Class<?> returnType, Class<?>... parameterTypes)
    {
        nodes.add(InvokeInstruction.invokeSpecial(type, name, returnType, parameterTypes));
        return this;
    }

    public BytecodeBlock invokeSpecial(Class<?> type, String name, Class<?> returnType, Iterable<Class<?>> parameterTypes)
    {
        nodes.add(InvokeInstruction.invokeSpecial(type, name, returnType, parameterTypes));
        return this;
    }

    public BytecodeBlock invokeSpecial(ParameterizedType type, String name, ParameterizedType returnType, ParameterizedType... parameterTypes)
    {
        nodes.add(InvokeInstruction.invokeSpecial(type, name, returnType, parameterTypes));
        return this;
    }

    public BytecodeBlock invokeSpecial(ParameterizedType type, String name, ParameterizedType returnType, Iterable<ParameterizedType> parameterTypes)
    {
        nodes.add(InvokeInstruction.invokeSpecial(type, name, returnType, parameterTypes));
        return this;
    }

    public BytecodeBlock invokeDynamic(String name, MethodType methodType, Method bootstrapMethod, Object... defaultBootstrapArguments)
    {
        nodes.add(InvokeInstruction.invokeDynamic(name, methodType, bootstrapMethod, defaultBootstrapArguments));
        return this;
    }

    public BytecodeNode invokeDynamic(String name,
            ParameterizedType returnType,
            Iterable<ParameterizedType> parameterTypes,
            Method bootstrapMethod,
            List<Object> bootstrapArgs)
    {
        nodes.add(InvokeInstruction.invokeDynamic(name, returnType, parameterTypes, bootstrapMethod, bootstrapArgs));
        return this;
    }

    public BytecodeBlock ret(Class<?> type)
    {
        if (type == long.class) {
            retLong();
        }
        else if (type == boolean.class) {
            retBoolean();
        }
        else if (type == int.class || type == byte.class || type == char.class || type == short.class) {
            retInt();
        }
        else if (type == float.class) {
            retFloat();
        }
        else if (type == double.class) {
            retDouble();
        }
        else if (type == void.class) {
            ret();
        }
        else if (!type.isPrimitive()) {
            retObject();
        }
        else {
            throw new IllegalArgumentException("Unsupported type: " + type.getName());
        }

        return this;
    }

    public BytecodeBlock ret()
    {
        nodes.add(OpCode.RETURN);
        return this;
    }

    public BytecodeBlock retObject()
    {
        nodes.add(OpCode.ARETURN);
        return this;
    }

    public BytecodeBlock retFloat()
    {
        nodes.add(OpCode.FRETURN);
        return this;
    }

    public BytecodeBlock retDouble()
    {
        nodes.add(OpCode.DRETURN);
        return this;
    }

    public BytecodeBlock retBoolean()
    {
        nodes.add(OpCode.IRETURN);
        return this;
    }

    public BytecodeBlock retLong()
    {
        nodes.add(OpCode.LRETURN);
        return this;
    }

    public BytecodeBlock retInt()
    {
        nodes.add(OpCode.IRETURN);
        return this;
    }

    public BytecodeBlock throwObject()
    {
        nodes.add(OpCode.ATHROW);
        return this;
    }

    public BytecodeBlock newObject(Class<?> type)
    {
        nodes.add(TypeInstruction.newObject(type));
        return this;
    }

    public BytecodeBlock newObject(ParameterizedType type)
    {
        nodes.add(TypeInstruction.newObject(type));
        return this;
    }

    public BytecodeBlock newArray(Class<?> type)
    {
        nodes.add(TypeInstruction.newObjectArray(type));
        return this;
    }

    public BytecodeBlock dup()
    {
        nodes.add(OpCode.DUP);
        return this;
    }

    public BytecodeBlock dup(Class<?> type)
    {
        if (type == long.class || type == double.class) {
            nodes.add(OpCode.DUP2);
        }
        else if (type != void.class) {
            nodes.add(OpCode.DUP);
        }
        return this;
    }

    public BytecodeBlock pop()
    {
        nodes.add(OpCode.POP);
        return this;
    }

    public BytecodeBlock pop(Class<?> type)
    {
        if (type == long.class || type == double.class) {
            nodes.add(OpCode.POP2);
        }
        else if (type != void.class) {
            nodes.add(OpCode.POP);
        }
        return this;
    }

    public BytecodeBlock pop(ParameterizedType type)
    {
        Class<?> primitiveType = type.getPrimitiveType();
        if (primitiveType == long.class || primitiveType == double.class) {
            nodes.add(OpCode.POP2);
        }
        else if (primitiveType != void.class) {
            nodes.add(OpCode.POP);
        }
        return this;
    }

    public BytecodeBlock swap()
    {
        nodes.add(OpCode.SWAP);
        return this;
    }

    //
    // Fields (non-static)
    //

    public BytecodeBlock getField(Field field)
    {
        return getField(field.getDeclaringClass(), field.getName(), field.getType());
    }

    public BytecodeBlock getField(FieldDefinition field)
    {
        getField(field.getDeclaringClass().getType(), field.getName(), field.getType());
        return this;
    }

    public BytecodeBlock getField(Class<?> target, String fieldName, Class<?> fieldType)
    {
        getField(type(target), fieldName, type(fieldType));
        return this;
    }

    public BytecodeBlock getField(ParameterizedType target, String fieldName, ParameterizedType fieldType)
    {
        nodes.add(getFieldInstruction(target, fieldName, fieldType));
        return this;
    }

    public BytecodeBlock putField(Field field)
    {
        return putField(field.getDeclaringClass(), field.getName(), field.getType());
    }

    public BytecodeBlock putField(Class<?> target, String fieldName, Class<?> fieldType)
    {
        putField(type(target), fieldName, type(fieldType));
        return this;
    }

    public BytecodeBlock putField(FieldDefinition field)
    {
        checkArgument(!field.getAccess().contains(STATIC), "Field is static: %s", field);
        putField(field.getDeclaringClass().getType(), field.getName(), field.getType());
        return this;
    }

    public BytecodeBlock putField(ParameterizedType target, String fieldName, ParameterizedType fieldType)
    {
        nodes.add(putFieldInstruction(target, fieldName, fieldType));
        return this;
    }

    //
    // Static fields
    //

    public BytecodeBlock getStaticField(FieldDefinition field)
    {
        getStaticField(field.getDeclaringClass().getType(), field.getName(), field.getType());
        return this;
    }

    public BytecodeBlock getStaticField(Field field)
    {
        checkArgument(Modifier.isStatic(field.getModifiers()), "Field is not static: %s", field);
        getStaticField(type(field.getDeclaringClass()), field.getName(), type(field.getType()));
        return this;
    }

    public BytecodeBlock getStaticField(Class<?> target, String fieldName, Class<?> fieldType)
    {
        nodes.add(getStaticInstruction(target, fieldName, fieldType));
        return this;
    }

    public BytecodeBlock getStaticField(ParameterizedType target, String fieldName, ParameterizedType fieldType)
    {
        nodes.add(getStaticInstruction(target, fieldName, fieldType));
        return this;
    }

    public BytecodeBlock getStaticField(ParameterizedType target, FieldDefinition field)
    {
        nodes.add(getStaticInstruction(target, field.getName(), field.getType()));
        return this;
    }

    public BytecodeBlock putStaticField(FieldDefinition field)
    {
        putStaticField(field.getDeclaringClass().getType(), field.getName(), field.getType());
        return this;
    }

    public BytecodeBlock putStaticField(ParameterizedType target, FieldDefinition field)
    {
        checkArgument(field.getAccess().contains(STATIC), "Field is not static: %s", field);
        putStaticField(target, field.getName(), field.getType());
        return this;
    }

    public BytecodeBlock putStaticField(ParameterizedType target, String fieldName, ParameterizedType fieldType)
    {
        nodes.add(putStaticInstruction(target, fieldName, fieldType));
        return this;
    }

    //
    // Load constants
    //

    public BytecodeBlock pushNull()
    {
        nodes.add(OpCode.ACONST_NULL);
        return this;
    }

    public BytecodeBlock push(Class<?> type)
    {
        nodes.add(loadClass(type));
        return this;
    }

    public BytecodeBlock push(ParameterizedType type)
    {
        nodes.add(loadClass(type));
        return this;
    }

    public BytecodeBlock push(String value)
    {
        nodes.add(Constant.loadString(value));
        return this;
    }

    public BytecodeBlock push(Number value)
    {
        nodes.add(loadNumber(value));
        return this;
    }

    public BytecodeBlock push(int value)
    {
        nodes.add(loadInt(value));
        return this;
    }

    public BytecodeBlock push(boolean value)
    {
        nodes.add(loadBoolean(value));
        return this;
    }

    public BytecodeBlock pushJavaDefault(Class<?> type)
    {
        if (type == void.class) {
            return this;
        }
        if (type == boolean.class || type == byte.class || type == char.class || type == short.class || type == int.class) {
            return push(0);
        }
        if (type == long.class) {
            return push(0L);
        }
        if (type == float.class) {
            return push(0.0f);
        }
        if (type == double.class) {
            return push(0.0d);
        }
        return pushNull();
    }

    public BytecodeBlock initializeVariable(Variable variable)
    {
        ParameterizedType type = variable.getType();
        if (type.getType().length() == 1) {
            switch (type.getType().charAt(0)) {
                case 'B':
                case 'Z':
                case 'S':
                case 'C':
                case 'I':
                    nodes.add(loadInt(0));
                    break;
                case 'F':
                    nodes.add(loadFloat(0));
                    break;
                case 'D':
                    nodes.add(loadDouble(0));
                    break;
                case 'J':
                    nodes.add(loadLong(0));
                    break;
                default:
                    checkArgument(false, "Unknown type '%s'", variable.getType());
            }
        }
        else {
            nodes.add(Constant.loadNull());
        }

        nodes.add(storeVariable(variable));

        return this;
    }

    public BytecodeBlock getVariable(Variable variable)
    {
        nodes.add(loadVariable(variable));
        return this;
    }

    public BytecodeBlock putVariable(Variable variable)
    {
        nodes.add(storeVariable(variable));
        return this;
    }

    public BytecodeBlock putVariable(Variable variable, Class<?> type)
    {
        nodes.add(loadClass(type));
        putVariable(variable);
        return this;
    }

    public BytecodeBlock putVariable(Variable variable, ParameterizedType type)
    {
        nodes.add(loadClass(type));
        putVariable(variable);
        return this;
    }

    public BytecodeBlock putVariable(Variable variable, String value)
    {
        nodes.add(Constant.loadString(value));
        putVariable(variable);
        return this;
    }

    public BytecodeBlock putVariable(Variable variable, Number value)
    {
        nodes.add(loadNumber(value));
        putVariable(variable);
        return this;
    }

    public BytecodeBlock putVariable(Variable variable, int value)
    {
        nodes.add(loadInt(value));
        putVariable(variable);
        return this;
    }

    public BytecodeBlock putVariable(Variable variable, boolean value)
    {
        nodes.add(loadBoolean(value));
        putVariable(variable);
        return this;
    }

    public BytecodeBlock incrementVariable(Variable variable, byte increment)
    {
        String type = variable.getType().getClassName();
        checkArgument(ImmutableList.of("byte", "short", "int").contains(type), "variable must be an byte, short or int, but is %s", type);
        nodes.add(VariableInstruction.incrementVariable(variable, increment));
        return this;
    }

    public BytecodeBlock getObjectArrayElement()
    {
        nodes.add(OpCode.AALOAD);
        return this;
    }

    public BytecodeBlock putObjectArrayElement()
    {
        nodes.add(OpCode.AASTORE);
        return this;
    }

    public BytecodeBlock visitLineNumber(int currentLineNumber)
    {
        checkArgument(currentLineNumber >= 0, "currentLineNumber must be positive");
        if (this.currentLineNumber != currentLineNumber) {
            nodes.add(new LineNumberNode(currentLineNumber));
            this.currentLineNumber = currentLineNumber;
        }
        return this;
    }

    @Override
    public void accept(MethodVisitor visitor, MethodGenerationContext generationContext)
    {
        for (BytecodeNode node : nodes) {
            node.accept(visitor, generationContext);
        }
    }

    @Override
    public <T> T accept(BytecodeNode parent, BytecodeVisitor<T> visitor)
    {
        return visitor.visitBlock(parent, this);
    }
}
