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
package com.facebook.presto.byteCode;

import com.facebook.presto.byteCode.debug.LineNumberNode;
import com.facebook.presto.byteCode.instruction.Constant;
import com.facebook.presto.byteCode.instruction.InvokeInstruction;
import com.facebook.presto.byteCode.instruction.JumpInstruction;
import com.facebook.presto.byteCode.instruction.LabelNode;
import com.facebook.presto.byteCode.instruction.TypeInstruction;
import com.facebook.presto.byteCode.instruction.VariableInstruction;
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

import static com.facebook.presto.byteCode.Access.STATIC;
import static com.facebook.presto.byteCode.ParameterizedType.type;
import static com.facebook.presto.byteCode.instruction.Constant.loadBoolean;
import static com.facebook.presto.byteCode.instruction.Constant.loadClass;
import static com.facebook.presto.byteCode.instruction.Constant.loadDouble;
import static com.facebook.presto.byteCode.instruction.Constant.loadFloat;
import static com.facebook.presto.byteCode.instruction.Constant.loadInt;
import static com.facebook.presto.byteCode.instruction.Constant.loadLong;
import static com.facebook.presto.byteCode.instruction.Constant.loadNumber;
import static com.facebook.presto.byteCode.instruction.FieldInstruction.getFieldInstruction;
import static com.facebook.presto.byteCode.instruction.FieldInstruction.getStaticInstruction;
import static com.facebook.presto.byteCode.instruction.FieldInstruction.putFieldInstruction;
import static com.facebook.presto.byteCode.instruction.FieldInstruction.putStaticInstruction;
import static com.facebook.presto.byteCode.instruction.TypeInstruction.cast;
import static com.facebook.presto.byteCode.instruction.TypeInstruction.instanceOf;
import static com.facebook.presto.byteCode.instruction.VariableInstruction.loadVariable;
import static com.facebook.presto.byteCode.instruction.VariableInstruction.storeVariable;
import static com.google.common.base.Preconditions.checkArgument;

@SuppressWarnings("UnusedDeclaration")
@NotThreadSafe
public class Block
        implements ByteCodeNode
{
    private final List<ByteCodeNode> nodes = new ArrayList<>();

    private String description;
    private int currentLineNumber = -1;

    public Block()
    {
    }

    public String getDescription()
    {
        return description;
    }

    public Block setDescription(String description)
    {
        this.description = description;
        return this;
    }

    @Override
    public List<ByteCodeNode> getChildNodes()
    {
        return ImmutableList.copyOf(nodes);
    }

    public Block append(ByteCodeNode node)
    {
        nodes.add(node);
        return this;
    }

    public Block comment(String comment)
    {
        nodes.add(new Comment(comment));
        return this;
    }

    public Block comment(String comment, Object... args)
    {
        nodes.add(new Comment(String.format(comment, args)));
        return this;
    }

    public boolean isEmpty()
    {
        return nodes.isEmpty();
    }

    public Block visitLabel(LabelNode label)
    {
        nodes.add(label);
        return this;
    }

    public Block gotoLabel(LabelNode label)
    {
        nodes.add(JumpInstruction.jump(label));
        return this;
    }

    public Block ifFalseGoto(LabelNode label)
    {
        return ifZeroGoto(label);
    }

    public Block ifTrueGoto(LabelNode label)
    {
        return ifNotZeroGoto(label);
    }

    public Block ifZeroGoto(LabelNode label)
    {
        nodes.add(JumpInstruction.jumpIfEqualZero(label));
        return this;
    }

    public Block ifNotZeroGoto(LabelNode label)
    {
        nodes.add(JumpInstruction.jumpIfNotEqualZero(label));
        return this;
    }

    public Block ifNullGoto(LabelNode label)
    {
        nodes.add(JumpInstruction.jumpIfNull(label));
        return this;
    }

    public Block ifNotNullGoto(LabelNode label)
    {
        nodes.add(JumpInstruction.jumpIfNotNull(label));
        return this;
    }

    public Block intAdd()
    {
        nodes.add(OpCode.IADD);
        return this;
    }

    public Block longAdd()
    {
        nodes.add(OpCode.LADD);
        return this;
    }

    public Block longCompare()
    {
        nodes.add(OpCode.LCMP);
        return this;
    }

    /**
     * Compare two doubles. If either is NaN comparison is -1.
     */
    public Block doubleCompareNanLess()
    {
        nodes.add(OpCode.DCMPL);
        return this;
    }

    /**
     * Compare two doubles. If either is NaN comparison is 1.
     */
    public Block doubleCompareNanGreater()
    {
        nodes.add(OpCode.DCMPG);
        return this;
    }

    public Block intLeftShift()
    {
        nodes.add(OpCode.ISHL);
        return this;
    }

    public Block intRightShift()
    {
        nodes.add(OpCode.ISHR);
        return this;
    }

    public Block longLeftShift()
    {
        nodes.add(OpCode.LSHL);
        return this;
    }

    public Block longRightShift()
    {
        nodes.add(OpCode.LSHR);
        return this;
    }

    public Block unsignedIntRightShift()
    {
        nodes.add(OpCode.IUSHR);
        return this;
    }

    public Block unsignedLongRightShift()
    {
        nodes.add(OpCode.LUSHR);
        return this;
    }

    public Block intBitAnd()
    {
        nodes.add(OpCode.IAND);
        return this;
    }

    public Block intBitOr()
    {
        nodes.add(OpCode.IOR);
        return this;
    }

    public Block intBitXor()
    {
        nodes.add(OpCode.IXOR);
        return this;
    }

    public Block longBitAnd()
    {
        nodes.add(OpCode.LAND);
        return this;
    }

    public Block longBitOr()
    {
        nodes.add(OpCode.LOR);
        return this;
    }

    public Block longBitXor()
    {
        nodes.add(OpCode.LXOR);
        return this;
    }

    public Block intNegate()
    {
        nodes.add(OpCode.INEG);
        return this;
    }

    public Block intToLong()
    {
        nodes.add(OpCode.I2L);
        return this;
    }

    public Block longNegate()
    {
        nodes.add(OpCode.LNEG);
        return this;
    }

    public Block longToInt()
    {
        nodes.add(OpCode.L2I);
        return this;
    }

    public Block isInstanceOf(Class<?> type)
    {
        nodes.add(instanceOf(type));
        return this;
    }

    public Block isInstanceOf(ParameterizedType type)
    {
        nodes.add(instanceOf(type));
        return this;
    }

    public Block checkCast(Class<?> type)
    {
        nodes.add(cast(type));
        return this;
    }

    public Block checkCast(ParameterizedType type)
    {
        nodes.add(cast(type));
        return this;
    }

    public Block invokeStatic(Method method)
    {
        nodes.add(InvokeInstruction.invokeStatic(method));
        return this;
    }

    public Block invokeStatic(MethodDefinition method)
    {
        nodes.add(InvokeInstruction.invokeStatic(method));
        return this;
    }

    public Block invokeStatic(Class<?> type, String name, Class<?> returnType, Class<?>... parameterTypes)
    {
        nodes.add(InvokeInstruction.invokeStatic(type, name, returnType, parameterTypes));
        return this;
    }

    public Block invokeStatic(Class<?> type, String name, Class<?> returnType, Iterable<Class<?>> parameterTypes)
    {
        nodes.add(InvokeInstruction.invokeStatic(type, name, returnType, parameterTypes));
        return this;
    }

    public Block invokeStatic(ParameterizedType type, String name, ParameterizedType returnType, ParameterizedType... parameterTypes)
    {
        nodes.add(InvokeInstruction.invokeStatic(type, name, returnType, parameterTypes));
        return this;
    }

    public Block invokeStatic(ParameterizedType type, String name, ParameterizedType returnType, Iterable<ParameterizedType> parameterTypes)
    {
        nodes.add(InvokeInstruction.invokeStatic(type, name, returnType, parameterTypes));
        return this;
    }

    public Block invokeVirtual(Method method)
    {
        nodes.add(InvokeInstruction.invokeVirtual(method));
        return this;
    }

    public Block invokeVirtual(MethodDefinition method)
    {
        nodes.add(InvokeInstruction.invokeVirtual(method));
        return this;
    }

    public Block invokeVirtual(Class<?> type, String name, Class<?> returnType, Class<?>... parameterTypes)
    {
        nodes.add(InvokeInstruction.invokeVirtual(type, name, returnType, parameterTypes));
        return this;
    }

    public Block invokeVirtual(Class<?> type, String name, Class<?> returnType, Iterable<Class<?>> parameterTypes)
    {
        nodes.add(InvokeInstruction.invokeVirtual(type, name, returnType, parameterTypes));
        return this;
    }

    public Block invokeVirtual(ParameterizedType type, String name, ParameterizedType returnType, ParameterizedType... parameterTypes)
    {
        nodes.add(InvokeInstruction.invokeVirtual(type, name, returnType, parameterTypes));
        return this;
    }

    public Block invokeVirtual(ParameterizedType type, String name, ParameterizedType returnType, Iterable<ParameterizedType> parameterTypes)
    {
        nodes.add(InvokeInstruction.invokeVirtual(type, name, returnType, parameterTypes));
        return this;
    }

    public Block invokeInterface(Method method)
    {
        nodes.add(InvokeInstruction.invokeInterface(method));
        return this;
    }

    public Block invokeInterface(MethodDefinition method)
    {
        nodes.add(InvokeInstruction.invokeInterface(method));
        return this;
    }

    public Block invokeInterface(Class<?> type, String name, Class<?> returnType, Class<?>... parameterTypes)
    {
        nodes.add(InvokeInstruction.invokeInterface(type, name, returnType, parameterTypes));
        return this;
    }

    public Block invokeInterface(Class<?> type, String name, Class<?> returnType, Iterable<Class<?>> parameterTypes)
    {
        nodes.add(InvokeInstruction.invokeInterface(type, name, returnType, parameterTypes));
        return this;
    }

    public Block invokeInterface(ParameterizedType type, String name, ParameterizedType returnType, ParameterizedType... parameterTypes)
    {
        nodes.add(InvokeInstruction.invokeInterface(type, name, returnType, parameterTypes));
        return this;
    }

    public Block invokeInterface(ParameterizedType type, String name, ParameterizedType returnType, Iterable<ParameterizedType> parameterTypes)
    {
        nodes.add(InvokeInstruction.invokeInterface(type, name, returnType, parameterTypes));
        return this;
    }

    public Block invokeConstructor(Constructor<?> constructor)
    {
        nodes.add(InvokeInstruction.invokeConstructor(constructor));
        return this;
    }

    public Block invokeConstructor(Class<?> type, Class<?>... parameterTypes)
    {
        nodes.add(InvokeInstruction.invokeConstructor(type, parameterTypes));
        return this;
    }

    public Block invokeConstructor(Class<?> type, Iterable<Class<?>> parameterTypes)
    {
        nodes.add(InvokeInstruction.invokeConstructor(type, parameterTypes));
        return this;
    }

    public Block invokeConstructor(ParameterizedType type, ParameterizedType... parameterTypes)
    {
        nodes.add(InvokeInstruction.invokeConstructor(type, parameterTypes));
        return this;
    }

    public Block invokeConstructor(ParameterizedType type, Iterable<ParameterizedType> parameterTypes)
    {
        nodes.add(InvokeInstruction.invokeConstructor(type, parameterTypes));
        return this;
    }

    public Block invokeSpecial(Method method)
    {
        nodes.add(InvokeInstruction.invokeSpecial(method));
        return this;
    }

    public Block invokeSpecial(MethodDefinition method)
    {
        nodes.add(InvokeInstruction.invokeSpecial(method));
        return this;
    }

    public Block invokeSpecial(Class<?> type, String name, Class<?> returnType, Class<?>... parameterTypes)
    {
        nodes.add(InvokeInstruction.invokeSpecial(type, name, returnType, parameterTypes));
        return this;
    }

    public Block invokeSpecial(Class<?> type, String name, Class<?> returnType, Iterable<Class<?>> parameterTypes)
    {
        nodes.add(InvokeInstruction.invokeSpecial(type, name, returnType, parameterTypes));
        return this;
    }

    public Block invokeSpecial(ParameterizedType type, String name, ParameterizedType returnType, ParameterizedType... parameterTypes)
    {
        nodes.add(InvokeInstruction.invokeSpecial(type, name, returnType, parameterTypes));
        return this;
    }

    public Block invokeSpecial(ParameterizedType type, String name, ParameterizedType returnType, Iterable<ParameterizedType> parameterTypes)
    {
        nodes.add(InvokeInstruction.invokeSpecial(type, name, returnType, parameterTypes));
        return this;
    }

    public Block invokeDynamic(String name, MethodType methodType, Method bootstrapMethod, Object... defaultBootstrapArguments)
    {
        nodes.add(InvokeInstruction.invokeDynamic(name, methodType, bootstrapMethod, defaultBootstrapArguments));
        return this;
    }

    public ByteCodeNode invokeDynamic(String name,
            ParameterizedType returnType,
            Iterable<ParameterizedType> parameterTypes,
            Method bootstrapMethod,
            List<Object> bootstrapArgs)
    {
        nodes.add(InvokeInstruction.invokeDynamic(name, returnType, parameterTypes, bootstrapMethod, bootstrapArgs));
        return this;
    }

    public Block ret(Class<?> type)
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

    public Block ret()
    {
        nodes.add(OpCode.RETURN);
        return this;
    }

    public Block retObject()
    {
        nodes.add(OpCode.ARETURN);
        return this;
    }

    public Block retFloat()
    {
        nodes.add(OpCode.FRETURN);
        return this;
    }

    public Block retDouble()
    {
        nodes.add(OpCode.DRETURN);
        return this;
    }

    public Block retBoolean()
    {
        nodes.add(OpCode.IRETURN);
        return this;
    }

    public Block retLong()
    {
        nodes.add(OpCode.LRETURN);
        return this;
    }

    public Block retInt()
    {
        nodes.add(OpCode.IRETURN);
        return this;
    }

    public Block throwObject()
    {
        nodes.add(OpCode.ATHROW);
        return this;
    }

    public Block newObject(Class<?> type)
    {
        nodes.add(TypeInstruction.newObject(type));
        return this;
    }

    public Block newObject(ParameterizedType type)
    {
        nodes.add(TypeInstruction.newObject(type));
        return this;
    }

    public Block newArray(Class<?> type)
    {
        nodes.add(TypeInstruction.newObjectArray(type));
        return this;
    }

    public Block dup()
    {
        nodes.add(OpCode.DUP);
        return this;
    }

    public Block dup(Class<?> type)
    {
        if (type == long.class || type == double.class) {
            nodes.add(OpCode.DUP2);
        }
        else {
            nodes.add(OpCode.DUP);
        }
        return this;
    }

    public Block pop()
    {
        nodes.add(OpCode.POP);
        return this;
    }

    public Block pop(Class<?> type)
    {
        if (type == long.class || type == double.class) {
            nodes.add(OpCode.POP2);
        }
        else if (type != void.class) {
            nodes.add(OpCode.POP);
        }
        return this;
    }

    public Block pop(ParameterizedType type)
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

    public Block swap()
    {
        nodes.add(OpCode.SWAP);
        return this;
    }

    //
    // Fields (non-static)
    //

    public Block getField(Field field)
    {
        return getField(field.getDeclaringClass(), field.getName(), field.getType());
    }

    public Block getField(FieldDefinition field)
    {
        getField(field.getDeclaringClass().getType(), field.getName(), field.getType());
        return this;
    }

    public Block getField(Class<?> target, String fieldName, Class<?> fieldType)
    {
        getField(type(target), fieldName, type(fieldType));
        return this;
    }

    public Block getField(ParameterizedType target, String fieldName, ParameterizedType fieldType)
    {
        nodes.add(getFieldInstruction(target, fieldName, fieldType));
        return this;
    }

    public Block putField(Field field)
    {
        return putField(field.getDeclaringClass(), field.getName(), field.getType());
    }

    public Block putField(Class<?> target, String fieldName, Class<?> fieldType)
    {
        putField(type(target), fieldName, type(fieldType));
        return this;
    }

    public Block putField(FieldDefinition field)
    {
        checkArgument(!field.getAccess().contains(STATIC), "Field is static: %s", field);
        putField(field.getDeclaringClass().getType(), field.getName(), field.getType());
        return this;
    }

    public Block putField(ParameterizedType target, String fieldName, ParameterizedType fieldType)
    {
        nodes.add(putFieldInstruction(target, fieldName, fieldType));
        return this;
    }

    //
    // Static fields
    //

    public Block getStaticField(FieldDefinition field)
    {
        getStaticField(field.getDeclaringClass().getType(), field.getName(), field.getType());
        return this;
    }

    public Block getStaticField(Field field)
    {
        checkArgument(Modifier.isStatic(field.getModifiers()), "Field is not static: %s", field);
        getStaticField(type(field.getDeclaringClass()), field.getName(), type(field.getType()));
        return this;
    }

    public Block getStaticField(Class<?> target, String fieldName, Class<?> fieldType)
    {
        nodes.add(getStaticInstruction(target, fieldName, fieldType));
        return this;
    }

    public Block getStaticField(ParameterizedType target, String fieldName, ParameterizedType fieldType)
    {
        nodes.add(getStaticInstruction(target, fieldName, fieldType));
        return this;
    }

    public Block getStaticField(ParameterizedType target, FieldDefinition field)
    {
        nodes.add(getStaticInstruction(target, field.getName(), field.getType()));
        return this;
    }

    public Block putStaticField(FieldDefinition field)
    {
        putStaticField(field.getDeclaringClass().getType(), field.getName(), field.getType());
        return this;
    }

    public Block putStaticField(ParameterizedType target, FieldDefinition field)
    {
        checkArgument(field.getAccess().contains(STATIC), "Field is not static: %s", field);
        putStaticField(target, field.getName(), field.getType());
        return this;
    }

    public Block putStaticField(ParameterizedType target, String fieldName, ParameterizedType fieldType)
    {
        nodes.add(putStaticInstruction(target, fieldName, fieldType));
        return this;
    }

    //
    // Load constants
    //

    public Block pushNull()
    {
        nodes.add(OpCode.ACONST_NULL);
        return this;
    }

    public Block push(Class<?> type)
    {
        nodes.add(loadClass(type));
        return this;
    }

    public Block push(ParameterizedType type)
    {
        nodes.add(loadClass(type));
        return this;
    }

    public Block push(String value)
    {
        nodes.add(Constant.loadString(value));
        return this;
    }

    public Block push(Number value)
    {
        nodes.add(loadNumber(value));
        return this;
    }

    public Block push(int value)
    {
        nodes.add(loadInt(value));
        return this;
    }

    public Block push(boolean value)
    {
        nodes.add(loadBoolean(value));
        return this;
    }

    public Block pushJavaDefault(Class<?> type)
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

    public Block initializeVariable(Variable variable)
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

    public Block getVariable(Variable variable)
    {
        nodes.add(loadVariable(variable));
        return this;
    }

    public Block putVariable(Variable variable)
    {
        nodes.add(storeVariable(variable));
        return this;
    }

    public Block putVariable(Variable variable, Class<?> type)
    {
        nodes.add(loadClass(type));
        putVariable(variable);
        return this;
    }

    public Block putVariable(Variable variable, ParameterizedType type)
    {
        nodes.add(loadClass(type));
        putVariable(variable);
        return this;
    }

    public Block putVariable(Variable variable, String value)
    {
        nodes.add(Constant.loadString(value));
        putVariable(variable);
        return this;
    }

    public Block putVariable(Variable variable, Number value)
    {
        nodes.add(loadNumber(value));
        putVariable(variable);
        return this;
    }

    public Block putVariable(Variable variable, int value)
    {
        nodes.add(loadInt(value));
        putVariable(variable);
        return this;
    }

    public Block putVariable(Variable variable, boolean value)
    {
        nodes.add(loadBoolean(value));
        putVariable(variable);
        return this;
    }

    public Block incrementVariable(Variable variable, byte increment)
    {
        String type = variable.getType().getClassName();
        checkArgument(ImmutableList.of("byte", "short", "int").contains(type), "variable must be an byte, short or int, but is %s", type);
        nodes.add(VariableInstruction.incrementVariable(variable, increment));
        return this;
    }

    public Block getObjectArrayElement()
    {
        nodes.add(OpCode.AALOAD);
        return this;
    }

    public Block putObjectArrayElement()
    {
        nodes.add(OpCode.AASTORE);
        return this;
    }

    public Block visitLineNumber(int currentLineNumber)
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
        for (ByteCodeNode node : nodes) {
            node.accept(visitor, generationContext);
        }
    }

    @Override
    public <T> T accept(ByteCodeNode parent, ByteCodeVisitor<T> visitor)
    {
        return visitor.visitBlock(parent, this);
    }
}
