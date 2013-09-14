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
import com.google.common.base.Preconditions;
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
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.invoke.MethodType.methodType;

@NotThreadSafe
public class Block
        implements ByteCodeNode
{
    private final CompilerContext context;
    private final List<ByteCodeNode> nodes = new ArrayList<>();
//    private final List<TryCatchBlockNode> tryCatchBlocks = new ArrayList<>();

    private String description;

    public Block(CompilerContext context)
    {
        this.context = context;
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
        if (node != OpCodes.NOP && !(node instanceof Block && ((Block) node).isEmpty())) {
            nodes.add(node);
        }
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
        return nodes.size() == 0;
    }

//    public List<TryCatchBlockNode> getTryCatchBlocks()
//    {
//        return tryCatchBlocks;
//    }
//
//    public BlockDefinition tryCatch(BlockDefinition tryBlock, BlockDefinition handlerBlock, ParameterizedType exceptionType)
//    {
//        LabelNode tryStart = new LabelNode();
//        LabelNode tryEnd = new LabelNode();
//        LabelNode handler = new LabelNode();
//        LabelNode done = new LabelNode();
//
//        String exceptionName = null;
//        if (exceptionType != null) {
//            exceptionName = exceptionType.getClassName();
//        }
//        tryCatchBlocks.add(new TryCatchBlockNode(tryStart, tryEnd, handler, exceptionName));
//
//        //
//        // try block
//        visitLabel(tryStart);
//        append(tryBlock);
//        visitLabel(tryEnd);
//        gotoLabel(done);
//
//        //
//        // handler block
//
//        visitLabel(handler);
//
//        // store exception
//        Variable exception = context.createTempVariable();
//        storeVariable(exception.getLocalVariableDefinition());
//
//        // execute handler code
//        append(handlerBlock);
//
//        // load and rethrow exception
//        loadVariable(exception.getLocalVariableDefinition());
//        context.dropTempVariable(exception);
//        throwObject();
//
//        // all done
//        visitLabel(done);
//
//        return this;
//    }

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

    public Block intLeftShift()
    {
        nodes.add(OpCodes.ISHL);
        return this;
    }

    public Block intRightShift()
    {
        nodes.add(OpCodes.ISHR);
        return this;
    }

    public Block longLeftShift()
    {
        nodes.add(OpCodes.LSHL);
        return this;
    }

    public Block longRightShift()
    {
        nodes.add(OpCodes.LSHR);
        return this;
    }

    public Block unsignedIntRightShift()
    {
        nodes.add(OpCodes.IUSHR);
        return this;
    }

    public Block unsignedLongRightShift()
    {
        nodes.add(OpCodes.LUSHR);
        return this;
    }

    public Block intBitAnd()
    {
        nodes.add(OpCodes.IAND);
        return this;
    }

    public Block intBitOr()
    {
        nodes.add(OpCodes.IOR);
        return this;
    }

    public Block intBitXor()
    {
        nodes.add(OpCodes.IXOR);
        return this;
    }

    public Block longBitAnd()
    {
        nodes.add(OpCodes.LAND);
        return this;
    }

    public Block longBitOr()
    {
        nodes.add(OpCodes.LOR);
        return this;
    }

    public Block longBitXor()
    {
        nodes.add(OpCodes.LXOR);
        return this;
    }

    public Block intNegate()
    {
        nodes.add(OpCodes.INEG);
        return this;
    }

    public Block longNegate()
    {
        nodes.add(OpCodes.LNEG);
        return this;
    }

    public Block longToInt()
    {
        nodes.add(OpCodes.L2I);
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

    public Block invokeDynamic(String name, Class<?> returnType, List<Class<?>> parameterTypes)
    {
        return invokeDynamic(name, methodType(returnType, parameterTypes));
    }

    public Block invokeDynamic(String name, Class<?> returnType, Class<?>... parameterTypes)
    {
        return invokeDynamic(name, methodType(returnType, ImmutableList.copyOf(parameterTypes)));
    }

    public Block invokeDynamic(String name, MethodType methodType)
    {
        return invokeDynamic(name, methodType, context.getDefaultBootstrapMethod(), context.getDefaultBootstrapArguments());
    }

    public Block invokeDynamic(String name, MethodType methodType, Object... defaultBootstrapArguments)
    {
        nodes.add(InvokeInstruction.invokeDynamic(name, methodType, context.getDefaultBootstrapMethod(), defaultBootstrapArguments));
        return this;
    }

    public Block invokeDynamic(String name, MethodType methodType, Method bootstrapMethod, Object... defaultBootstrapArguments)
    {
        nodes.add(InvokeInstruction.invokeDynamic(name, methodType, bootstrapMethod, defaultBootstrapArguments));
        return this;
    }

    public Block ret()
    {
        nodes.add(OpCodes.RETURN);
        return this;
    }

    public Block retObject()
    {
        nodes.add(OpCodes.ARETURN);
        return this;
    }

    public Block retBoolean()
    {
        nodes.add(OpCodes.IRETURN);
        return this;
    }

    public Block retLong()
    {
        nodes.add(OpCodes.LRETURN);
        return this;
    }

    public Block retInt()
    {
        nodes.add(OpCodes.IRETURN);
        return this;
    }

    public Block throwObject()
    {
        nodes.add(OpCodes.ATHROW);
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
        nodes.add(OpCodes.DUP);
        return this;
    }

    public Block dup(Class<?> type)
    {
        if (type == long.class || type == double.class) {
            nodes.add(OpCodes.DUP2);
        }
        else {
            nodes.add(OpCodes.DUP);
        }
        return this;
    }

    public Block pop()
    {
        nodes.add(OpCodes.POP);
        return this;
    }

    public Block pop(Class<?> type)
    {
        if (type == long.class || type == double.class) {
            nodes.add(OpCodes.POP2);
        }
        else if (type != void.class) {
            nodes.add(OpCodes.POP);
        }
        return this;
    }

    public Block swap()
    {
        nodes.add(OpCodes.SWAP);
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

    public Block pushThis()
    {
        getVariable("this");
        return this;
    }

    public Block pushNull()
    {
        nodes.add(OpCodes.ACONST_NULL);
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

    public Block getVariable(String name)
    {
        append(context.getVariable(name).getValue());
        return this;
    }

    public Block getVariable(String name, ParameterizedType type)
    {
        getVariable(name);
        checkCast(type);
        return this;
    }

    public Block initializeVariable(LocalVariableDefinition variable)
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

        nodes.add(VariableInstruction.storeVariable(variable));

        return this;
    }

    public Block getVariable(LocalVariableDefinition variable)
    {
        nodes.add(VariableInstruction.loadVariable(variable));
        return this;
    }

    public Block putVariable(String name)
    {
        append(context.getVariable(name).setValue());
        return this;
    }

    public Block putVariable(String name, Class<?> type)
    {
        nodes.add(loadClass(type));
        putVariable(name);
        return this;
    }

    public Block putVariable(String name, ParameterizedType type)
    {
        nodes.add(loadClass(type));
        putVariable(name);
        return this;
    }

    public Block putVariable(String name, String value)
    {
        nodes.add(Constant.loadString(value));
        putVariable(name);
        return this;
    }

    public Block putVariable(String name, Number value)
    {
        nodes.add(loadNumber(value));
        putVariable(name);
        return this;
    }

    public Block putVariable(String name, int value)
    {
        nodes.add(loadInt(value));
        putVariable(name);
        return this;
    }

    public Block putVariable(String name, boolean value)
    {
        nodes.add(loadBoolean(value));
        putVariable(name);
        return this;
    }

    public Block putVariable(LocalVariableDefinition variable)
    {
        nodes.add(VariableInstruction.storeVariable(variable));
        return this;
    }

    public Block putVariable(LocalVariableDefinition variable, Class<?> type)
    {
        nodes.add(loadClass(type));
        putVariable(variable);
        return this;
    }

    public Block putVariable(LocalVariableDefinition variable, ParameterizedType type)
    {
        nodes.add(loadClass(type));
        putVariable(variable);
        return this;
    }

    public Block putVariable(LocalVariableDefinition variable, String value)
    {
        nodes.add(Constant.loadString(value));
        putVariable(variable);
        return this;
    }

    public Block putVariable(LocalVariableDefinition variable, Number value)
    {
        nodes.add(loadNumber(value));
        putVariable(variable);
        return this;
    }

    public Block putVariable(LocalVariableDefinition variable, int value)
    {
        nodes.add(loadInt(value));
        putVariable(variable);
        return this;
    }

    public Block putVariable(LocalVariableDefinition variable, boolean value)
    {
        nodes.add(loadBoolean(value));
        putVariable(variable);
        return this;
    }

    public Block incrementVariable(LocalVariableDefinition variable, byte increment)
    {
        String type = variable.getType().getClassName();
        Preconditions.checkArgument(ImmutableList.of("byte", "short", "int").contains(type), "variable must be an byte, short or int, but is %s", type);
        nodes.add(VariableInstruction.incrementVariable(variable, increment));
        return this;
    }

    public Block getObjectArrayElement()
    {
        nodes.add(OpCodes.AALOAD);
        return this;
    }

    public Block putObjectArrayElement()
    {
        nodes.add(OpCodes.AASTORE);
        return this;
    }

    public Block visitLineNumber(int line)
    {
        if (line <= 0) {
            context.cleanLineNumber();
        }
        else if (!context.hasVisitedLine(line)) {
            nodes.add(new LineNumberNode(line));
            context.visitLine(line);
        }
        return this;
    }

    @Override
    public void accept(MethodVisitor visitor)
    {
        for (ByteCodeNode node : nodes) {
            node.accept(visitor);
        }
    }

    @Override
    public <T> T accept(ByteCodeNode parent, ByteCodeVisitor<T> visitor)
    {
        return visitor.visitBlock(parent, this);
    }
}
