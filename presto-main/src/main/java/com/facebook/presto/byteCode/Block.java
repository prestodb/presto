/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.byteCode;

import com.google.common.collect.ImmutableList;
import org.objectweb.asm.MethodVisitor;
import com.facebook.presto.byteCode.debug.LineNumberNode;
import com.facebook.presto.byteCode.instruction.Constant;
import com.facebook.presto.byteCode.instruction.InvokeInstruction;
import com.facebook.presto.byteCode.instruction.JumpInstruction;
import com.facebook.presto.byteCode.instruction.LabelNode;
import com.facebook.presto.byteCode.instruction.TypeInstruction;
import com.facebook.presto.byteCode.instruction.VariableInstruction;

import javax.annotation.concurrent.NotThreadSafe;
import java.lang.invoke.MethodType;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.invoke.MethodType.methodType;
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

@NotThreadSafe
public class Block implements ByteCodeNode
{
    private final CompilerContext context;
    private final List<ByteCodeNode> nodes = new ArrayList<>();
//    private final List<TryCatchBlockNode> tryCatchBlocks = new ArrayList<>();

    private String description;
    private String comment;

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

    public String getComment()
    {
        return comment;
    }

    public Block setComment(String comment)
    {
        this.comment = comment;
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

    public Block loadThis()
    {
        loadVariable("this");
        return this;
    }

    public Block loadObject(int slot)
    {
        nodes.add(VariableInstruction.loadVariable(slot));
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

    public Block invokeDynamic(String name, MethodType methodType, Method defaultBootstrapMethod, Object... defaultBootstrapArguments)
    {
        nodes.add(InvokeInstruction.invokeDynamic(name, methodType, defaultBootstrapMethod, defaultBootstrapArguments));
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
        else {
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

    public Block getField(Class<?> target, FieldDefinition field)
    {
        getField(type(target), field.getName(), field.getType());
        return this;
    }

    public Block getField(ParameterizedType target, FieldDefinition field)
    {
        getField(target, field.getName(), field.getType());
        return this;
    }

    public Block getField(Class<?> target, String fieldName, Class<?> fieldType)
    {
        getField(type(target), fieldName, type(fieldType));
        return this;
    }

    public Block getField(ParameterizedType target, String fieldName, ParameterizedType fieldType
    )
    {
        nodes.add(getFieldInstruction(target, fieldName, fieldType));
        return this;
    }

    public Block putField(Field field)
    {
        return putField(field.getDeclaringClass(), field.getName(), field.getType());
    }

    public Block putField(Class<?> target, FieldDefinition field)
    {
        return putField(type(target), field);
    }

    public Block putField(Class<?> target, String fieldName, Class<?> fieldType)
    {
        putField(type(target), fieldName, type(fieldType));
        return this;
    }

    public Block putField(ParameterizedType target, FieldDefinition field)
    {
        checkArgument(!field.getAccess().contains(STATIC), "Field is static: %s", field);
        putField(target, field.getName(), field.getType());
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

    public Block getStaticField(ParameterizedType target, String fieldName, ParameterizedType fieldType)
    {
        nodes.add(getStaticInstruction(target, fieldName, fieldType));
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

    public Block loadNull()
    {
        nodes.add(OpCodes.ACONST_NULL);
        return this;
    }

    public Block loadConstant(Class<?> type)
    {
        nodes.add(loadClass(type));
        return this;
    }

    public Block loadConstant(ParameterizedType type)
    {
        nodes.add(loadClass(type));
        return this;
    }

    public Block loadString(String value)
    {
        nodes.add(Constant.loadString(value));
        return this;
    }

    public Block loadConstant(Number value)
    {
        nodes.add(loadNumber(value));
        return this;
    }

    public Block loadConstant(int value)
    {
        nodes.add(loadInt(value));
        return this;
    }

    public Block loadConstant(boolean value)
    {
        nodes.add(loadBoolean(value));
        return this;
    }

    public Block loadVariable(String name)
    {
        append(context.getVariable(name).getValue());
        return this;
    }

    public Block loadVariable(String name, ParameterizedType type)
    {
        loadVariable(name);
        checkCast(type);
        return this;
    }

    public Block initializeLocalVariable(LocalVariableDefinition variable)
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

    public Block loadVariable(LocalVariableDefinition variable)
    {
        nodes.add(VariableInstruction.loadVariable(variable));
        return this;
    }

    public Block storeVariable(String name)
    {
        append(context.getVariable(name).setValue());
        return this;
    }

    public Block storeVariable(LocalVariableDefinition variable)
    {
        nodes.add(VariableInstruction.storeVariable(variable));
        return this;
    }

    public Block loadObjectArray()
    {
        nodes.add(OpCodes.AALOAD);
        return this;
    }

    public Block storeObjectArray()
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
