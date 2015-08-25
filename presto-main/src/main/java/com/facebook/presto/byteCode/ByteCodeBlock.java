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
public class ByteCodeBlock
        implements ByteCodeNode
{
    private final List<ByteCodeNode> nodes = new ArrayList<>();

    private String description;
    private int currentLineNumber = -1;

    public String getDescription()
    {
        return description;
    }

    public ByteCodeBlock setDescription(String description)
    {
        this.description = description;
        return this;
    }

    @Override
    public List<ByteCodeNode> getChildNodes()
    {
        return ImmutableList.copyOf(nodes);
    }

    public ByteCodeBlock append(ByteCodeNode node)
    {
        nodes.add(node);
        return this;
    }

    public ByteCodeBlock comment(String comment)
    {
        nodes.add(new Comment(comment));
        return this;
    }

    public ByteCodeBlock comment(String comment, Object... args)
    {
        nodes.add(new Comment(String.format(comment, args)));
        return this;
    }

    public boolean isEmpty()
    {
        return nodes.isEmpty();
    }

    public ByteCodeBlock visitLabel(LabelNode label)
    {
        nodes.add(label);
        return this;
    }

    public ByteCodeBlock gotoLabel(LabelNode label)
    {
        nodes.add(JumpInstruction.jump(label));
        return this;
    }

    public ByteCodeBlock ifFalseGoto(LabelNode label)
    {
        return ifZeroGoto(label);
    }

    public ByteCodeBlock ifTrueGoto(LabelNode label)
    {
        return ifNotZeroGoto(label);
    }

    public ByteCodeBlock ifZeroGoto(LabelNode label)
    {
        nodes.add(JumpInstruction.jumpIfEqualZero(label));
        return this;
    }

    public ByteCodeBlock ifNotZeroGoto(LabelNode label)
    {
        nodes.add(JumpInstruction.jumpIfNotEqualZero(label));
        return this;
    }

    public ByteCodeBlock ifNullGoto(LabelNode label)
    {
        nodes.add(JumpInstruction.jumpIfNull(label));
        return this;
    }

    public ByteCodeBlock ifNotNullGoto(LabelNode label)
    {
        nodes.add(JumpInstruction.jumpIfNotNull(label));
        return this;
    }

    public ByteCodeBlock intAdd()
    {
        nodes.add(OpCode.IADD);
        return this;
    }

    public ByteCodeBlock longAdd()
    {
        nodes.add(OpCode.LADD);
        return this;
    }

    public ByteCodeBlock longCompare()
    {
        nodes.add(OpCode.LCMP);
        return this;
    }

    /**
     * Compare two doubles. If either is NaN comparison is -1.
     */
    public ByteCodeBlock doubleCompareNanLess()
    {
        nodes.add(OpCode.DCMPL);
        return this;
    }

    /**
     * Compare two doubles. If either is NaN comparison is 1.
     */
    public ByteCodeBlock doubleCompareNanGreater()
    {
        nodes.add(OpCode.DCMPG);
        return this;
    }

    public ByteCodeBlock intLeftShift()
    {
        nodes.add(OpCode.ISHL);
        return this;
    }

    public ByteCodeBlock intRightShift()
    {
        nodes.add(OpCode.ISHR);
        return this;
    }

    public ByteCodeBlock longLeftShift()
    {
        nodes.add(OpCode.LSHL);
        return this;
    }

    public ByteCodeBlock longRightShift()
    {
        nodes.add(OpCode.LSHR);
        return this;
    }

    public ByteCodeBlock unsignedIntRightShift()
    {
        nodes.add(OpCode.IUSHR);
        return this;
    }

    public ByteCodeBlock unsignedLongRightShift()
    {
        nodes.add(OpCode.LUSHR);
        return this;
    }

    public ByteCodeBlock intBitAnd()
    {
        nodes.add(OpCode.IAND);
        return this;
    }

    public ByteCodeBlock intBitOr()
    {
        nodes.add(OpCode.IOR);
        return this;
    }

    public ByteCodeBlock intBitXor()
    {
        nodes.add(OpCode.IXOR);
        return this;
    }

    public ByteCodeBlock longBitAnd()
    {
        nodes.add(OpCode.LAND);
        return this;
    }

    public ByteCodeBlock longBitOr()
    {
        nodes.add(OpCode.LOR);
        return this;
    }

    public ByteCodeBlock longBitXor()
    {
        nodes.add(OpCode.LXOR);
        return this;
    }

    public ByteCodeBlock intNegate()
    {
        nodes.add(OpCode.INEG);
        return this;
    }

    public ByteCodeBlock intToLong()
    {
        nodes.add(OpCode.I2L);
        return this;
    }

    public ByteCodeBlock longNegate()
    {
        nodes.add(OpCode.LNEG);
        return this;
    }

    public ByteCodeBlock longToInt()
    {
        nodes.add(OpCode.L2I);
        return this;
    }

    public ByteCodeBlock isInstanceOf(Class<?> type)
    {
        nodes.add(instanceOf(type));
        return this;
    }

    public ByteCodeBlock isInstanceOf(ParameterizedType type)
    {
        nodes.add(instanceOf(type));
        return this;
    }

    public ByteCodeBlock checkCast(Class<?> type)
    {
        nodes.add(cast(type));
        return this;
    }

    public ByteCodeBlock checkCast(ParameterizedType type)
    {
        nodes.add(cast(type));
        return this;
    }

    public ByteCodeBlock invokeStatic(Method method)
    {
        nodes.add(InvokeInstruction.invokeStatic(method));
        return this;
    }

    public ByteCodeBlock invokeStatic(MethodDefinition method)
    {
        nodes.add(InvokeInstruction.invokeStatic(method));
        return this;
    }

    public ByteCodeBlock invokeStatic(Class<?> type, String name, Class<?> returnType, Class<?>... parameterTypes)
    {
        nodes.add(InvokeInstruction.invokeStatic(type, name, returnType, parameterTypes));
        return this;
    }

    public ByteCodeBlock invokeStatic(Class<?> type, String name, Class<?> returnType, Iterable<Class<?>> parameterTypes)
    {
        nodes.add(InvokeInstruction.invokeStatic(type, name, returnType, parameterTypes));
        return this;
    }

    public ByteCodeBlock invokeStatic(ParameterizedType type, String name, ParameterizedType returnType, ParameterizedType... parameterTypes)
    {
        nodes.add(InvokeInstruction.invokeStatic(type, name, returnType, parameterTypes));
        return this;
    }

    public ByteCodeBlock invokeStatic(ParameterizedType type, String name, ParameterizedType returnType, Iterable<ParameterizedType> parameterTypes)
    {
        nodes.add(InvokeInstruction.invokeStatic(type, name, returnType, parameterTypes));
        return this;
    }

    public ByteCodeBlock invokeVirtual(Method method)
    {
        nodes.add(InvokeInstruction.invokeVirtual(method));
        return this;
    }

    public ByteCodeBlock invokeVirtual(MethodDefinition method)
    {
        nodes.add(InvokeInstruction.invokeVirtual(method));
        return this;
    }

    public ByteCodeBlock invokeVirtual(Class<?> type, String name, Class<?> returnType, Class<?>... parameterTypes)
    {
        nodes.add(InvokeInstruction.invokeVirtual(type, name, returnType, parameterTypes));
        return this;
    }

    public ByteCodeBlock invokeVirtual(Class<?> type, String name, Class<?> returnType, Iterable<Class<?>> parameterTypes)
    {
        nodes.add(InvokeInstruction.invokeVirtual(type, name, returnType, parameterTypes));
        return this;
    }

    public ByteCodeBlock invokeVirtual(ParameterizedType type, String name, ParameterizedType returnType, ParameterizedType... parameterTypes)
    {
        nodes.add(InvokeInstruction.invokeVirtual(type, name, returnType, parameterTypes));
        return this;
    }

    public ByteCodeBlock invokeVirtual(ParameterizedType type, String name, ParameterizedType returnType, Iterable<ParameterizedType> parameterTypes)
    {
        nodes.add(InvokeInstruction.invokeVirtual(type, name, returnType, parameterTypes));
        return this;
    }

    public ByteCodeBlock invokeInterface(Method method)
    {
        nodes.add(InvokeInstruction.invokeInterface(method));
        return this;
    }

    public ByteCodeBlock invokeInterface(MethodDefinition method)
    {
        nodes.add(InvokeInstruction.invokeInterface(method));
        return this;
    }

    public ByteCodeBlock invokeInterface(Class<?> type, String name, Class<?> returnType, Class<?>... parameterTypes)
    {
        nodes.add(InvokeInstruction.invokeInterface(type, name, returnType, parameterTypes));
        return this;
    }

    public ByteCodeBlock invokeInterface(Class<?> type, String name, Class<?> returnType, Iterable<Class<?>> parameterTypes)
    {
        nodes.add(InvokeInstruction.invokeInterface(type, name, returnType, parameterTypes));
        return this;
    }

    public ByteCodeBlock invokeInterface(ParameterizedType type, String name, ParameterizedType returnType, ParameterizedType... parameterTypes)
    {
        nodes.add(InvokeInstruction.invokeInterface(type, name, returnType, parameterTypes));
        return this;
    }

    public ByteCodeBlock invokeInterface(ParameterizedType type, String name, ParameterizedType returnType, Iterable<ParameterizedType> parameterTypes)
    {
        nodes.add(InvokeInstruction.invokeInterface(type, name, returnType, parameterTypes));
        return this;
    }

    public ByteCodeBlock invokeConstructor(Constructor<?> constructor)
    {
        nodes.add(InvokeInstruction.invokeConstructor(constructor));
        return this;
    }

    public ByteCodeBlock invokeConstructor(Class<?> type, Class<?>... parameterTypes)
    {
        nodes.add(InvokeInstruction.invokeConstructor(type, parameterTypes));
        return this;
    }

    public ByteCodeBlock invokeConstructor(Class<?> type, Iterable<Class<?>> parameterTypes)
    {
        nodes.add(InvokeInstruction.invokeConstructor(type, parameterTypes));
        return this;
    }

    public ByteCodeBlock invokeConstructor(ParameterizedType type, ParameterizedType... parameterTypes)
    {
        nodes.add(InvokeInstruction.invokeConstructor(type, parameterTypes));
        return this;
    }

    public ByteCodeBlock invokeConstructor(ParameterizedType type, Iterable<ParameterizedType> parameterTypes)
    {
        nodes.add(InvokeInstruction.invokeConstructor(type, parameterTypes));
        return this;
    }

    public ByteCodeBlock invokeSpecial(Method method)
    {
        nodes.add(InvokeInstruction.invokeSpecial(method));
        return this;
    }

    public ByteCodeBlock invokeSpecial(MethodDefinition method)
    {
        nodes.add(InvokeInstruction.invokeSpecial(method));
        return this;
    }

    public ByteCodeBlock invokeSpecial(Class<?> type, String name, Class<?> returnType, Class<?>... parameterTypes)
    {
        nodes.add(InvokeInstruction.invokeSpecial(type, name, returnType, parameterTypes));
        return this;
    }

    public ByteCodeBlock invokeSpecial(Class<?> type, String name, Class<?> returnType, Iterable<Class<?>> parameterTypes)
    {
        nodes.add(InvokeInstruction.invokeSpecial(type, name, returnType, parameterTypes));
        return this;
    }

    public ByteCodeBlock invokeSpecial(ParameterizedType type, String name, ParameterizedType returnType, ParameterizedType... parameterTypes)
    {
        nodes.add(InvokeInstruction.invokeSpecial(type, name, returnType, parameterTypes));
        return this;
    }

    public ByteCodeBlock invokeSpecial(ParameterizedType type, String name, ParameterizedType returnType, Iterable<ParameterizedType> parameterTypes)
    {
        nodes.add(InvokeInstruction.invokeSpecial(type, name, returnType, parameterTypes));
        return this;
    }

    public ByteCodeBlock invokeDynamic(String name, MethodType methodType, Method bootstrapMethod, Object... defaultBootstrapArguments)
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

    public ByteCodeBlock ret(Class<?> type)
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

    public ByteCodeBlock ret()
    {
        nodes.add(OpCode.RETURN);
        return this;
    }

    public ByteCodeBlock retObject()
    {
        nodes.add(OpCode.ARETURN);
        return this;
    }

    public ByteCodeBlock retFloat()
    {
        nodes.add(OpCode.FRETURN);
        return this;
    }

    public ByteCodeBlock retDouble()
    {
        nodes.add(OpCode.DRETURN);
        return this;
    }

    public ByteCodeBlock retBoolean()
    {
        nodes.add(OpCode.IRETURN);
        return this;
    }

    public ByteCodeBlock retLong()
    {
        nodes.add(OpCode.LRETURN);
        return this;
    }

    public ByteCodeBlock retInt()
    {
        nodes.add(OpCode.IRETURN);
        return this;
    }

    public ByteCodeBlock throwObject()
    {
        nodes.add(OpCode.ATHROW);
        return this;
    }

    public ByteCodeBlock newObject(Class<?> type)
    {
        nodes.add(TypeInstruction.newObject(type));
        return this;
    }

    public ByteCodeBlock newObject(ParameterizedType type)
    {
        nodes.add(TypeInstruction.newObject(type));
        return this;
    }

    public ByteCodeBlock newArray(Class<?> type)
    {
        nodes.add(TypeInstruction.newObjectArray(type));
        return this;
    }

    public ByteCodeBlock dup()
    {
        nodes.add(OpCode.DUP);
        return this;
    }

    public ByteCodeBlock dup(Class<?> type)
    {
        if (type == long.class || type == double.class) {
            nodes.add(OpCode.DUP2);
        }
        else {
            nodes.add(OpCode.DUP);
        }
        return this;
    }

    public ByteCodeBlock pop()
    {
        nodes.add(OpCode.POP);
        return this;
    }

    public ByteCodeBlock pop(Class<?> type)
    {
        if (type == long.class || type == double.class) {
            nodes.add(OpCode.POP2);
        }
        else if (type != void.class) {
            nodes.add(OpCode.POP);
        }
        return this;
    }

    public ByteCodeBlock pop(ParameterizedType type)
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

    public ByteCodeBlock swap()
    {
        nodes.add(OpCode.SWAP);
        return this;
    }

    //
    // Fields (non-static)
    //

    public ByteCodeBlock getField(Field field)
    {
        return getField(field.getDeclaringClass(), field.getName(), field.getType());
    }

    public ByteCodeBlock getField(FieldDefinition field)
    {
        getField(field.getDeclaringClass().getType(), field.getName(), field.getType());
        return this;
    }

    public ByteCodeBlock getField(Class<?> target, String fieldName, Class<?> fieldType)
    {
        getField(type(target), fieldName, type(fieldType));
        return this;
    }

    public ByteCodeBlock getField(ParameterizedType target, String fieldName, ParameterizedType fieldType)
    {
        nodes.add(getFieldInstruction(target, fieldName, fieldType));
        return this;
    }

    public ByteCodeBlock putField(Field field)
    {
        return putField(field.getDeclaringClass(), field.getName(), field.getType());
    }

    public ByteCodeBlock putField(Class<?> target, String fieldName, Class<?> fieldType)
    {
        putField(type(target), fieldName, type(fieldType));
        return this;
    }

    public ByteCodeBlock putField(FieldDefinition field)
    {
        checkArgument(!field.getAccess().contains(STATIC), "Field is static: %s", field);
        putField(field.getDeclaringClass().getType(), field.getName(), field.getType());
        return this;
    }

    public ByteCodeBlock putField(ParameterizedType target, String fieldName, ParameterizedType fieldType)
    {
        nodes.add(putFieldInstruction(target, fieldName, fieldType));
        return this;
    }

    //
    // Static fields
    //

    public ByteCodeBlock getStaticField(FieldDefinition field)
    {
        getStaticField(field.getDeclaringClass().getType(), field.getName(), field.getType());
        return this;
    }

    public ByteCodeBlock getStaticField(Field field)
    {
        checkArgument(Modifier.isStatic(field.getModifiers()), "Field is not static: %s", field);
        getStaticField(type(field.getDeclaringClass()), field.getName(), type(field.getType()));
        return this;
    }

    public ByteCodeBlock getStaticField(Class<?> target, String fieldName, Class<?> fieldType)
    {
        nodes.add(getStaticInstruction(target, fieldName, fieldType));
        return this;
    }

    public ByteCodeBlock getStaticField(ParameterizedType target, String fieldName, ParameterizedType fieldType)
    {
        nodes.add(getStaticInstruction(target, fieldName, fieldType));
        return this;
    }

    public ByteCodeBlock getStaticField(ParameterizedType target, FieldDefinition field)
    {
        nodes.add(getStaticInstruction(target, field.getName(), field.getType()));
        return this;
    }

    public ByteCodeBlock putStaticField(FieldDefinition field)
    {
        putStaticField(field.getDeclaringClass().getType(), field.getName(), field.getType());
        return this;
    }

    public ByteCodeBlock putStaticField(ParameterizedType target, FieldDefinition field)
    {
        checkArgument(field.getAccess().contains(STATIC), "Field is not static: %s", field);
        putStaticField(target, field.getName(), field.getType());
        return this;
    }

    public ByteCodeBlock putStaticField(ParameterizedType target, String fieldName, ParameterizedType fieldType)
    {
        nodes.add(putStaticInstruction(target, fieldName, fieldType));
        return this;
    }

    //
    // Load constants
    //

    public ByteCodeBlock pushNull()
    {
        nodes.add(OpCode.ACONST_NULL);
        return this;
    }

    public ByteCodeBlock push(Class<?> type)
    {
        nodes.add(loadClass(type));
        return this;
    }

    public ByteCodeBlock push(ParameterizedType type)
    {
        nodes.add(loadClass(type));
        return this;
    }

    public ByteCodeBlock push(String value)
    {
        nodes.add(Constant.loadString(value));
        return this;
    }

    public ByteCodeBlock push(Number value)
    {
        nodes.add(loadNumber(value));
        return this;
    }

    public ByteCodeBlock push(int value)
    {
        nodes.add(loadInt(value));
        return this;
    }

    public ByteCodeBlock push(boolean value)
    {
        nodes.add(loadBoolean(value));
        return this;
    }

    public ByteCodeBlock pushJavaDefault(Class<?> type)
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

    public ByteCodeBlock initializeVariable(Variable variable)
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

    public ByteCodeBlock getVariable(Variable variable)
    {
        nodes.add(loadVariable(variable));
        return this;
    }

    public ByteCodeBlock putVariable(Variable variable)
    {
        nodes.add(storeVariable(variable));
        return this;
    }

    public ByteCodeBlock putVariable(Variable variable, Class<?> type)
    {
        nodes.add(loadClass(type));
        putVariable(variable);
        return this;
    }

    public ByteCodeBlock putVariable(Variable variable, ParameterizedType type)
    {
        nodes.add(loadClass(type));
        putVariable(variable);
        return this;
    }

    public ByteCodeBlock putVariable(Variable variable, String value)
    {
        nodes.add(Constant.loadString(value));
        putVariable(variable);
        return this;
    }

    public ByteCodeBlock putVariable(Variable variable, Number value)
    {
        nodes.add(loadNumber(value));
        putVariable(variable);
        return this;
    }

    public ByteCodeBlock putVariable(Variable variable, int value)
    {
        nodes.add(loadInt(value));
        putVariable(variable);
        return this;
    }

    public ByteCodeBlock putVariable(Variable variable, boolean value)
    {
        nodes.add(loadBoolean(value));
        putVariable(variable);
        return this;
    }

    public ByteCodeBlock incrementVariable(Variable variable, byte increment)
    {
        String type = variable.getType().getClassName();
        checkArgument(ImmutableList.of("byte", "short", "int").contains(type), "variable must be an byte, short or int, but is %s", type);
        nodes.add(VariableInstruction.incrementVariable(variable, increment));
        return this;
    }

    public ByteCodeBlock getObjectArrayElement()
    {
        nodes.add(OpCode.AALOAD);
        return this;
    }

    public ByteCodeBlock putObjectArrayElement()
    {
        nodes.add(OpCode.AASTORE);
        return this;
    }

    public ByteCodeBlock visitLineNumber(int currentLineNumber)
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
