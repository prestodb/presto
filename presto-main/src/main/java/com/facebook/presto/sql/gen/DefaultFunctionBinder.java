package com.facebook.presto.sql.gen;

import com.facebook.presto.byteCode.ByteCodeNode;
import com.facebook.presto.byteCode.instruction.Constant;
import com.facebook.presto.sql.gen.ExpressionCompiler.TypedByteCodeNode;
import com.google.common.collect.Ordering;

import java.lang.invoke.CallSite;
import java.lang.invoke.ConstantCallSite;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

public class DefaultFunctionBinder
        implements FunctionBinder
{
    private final MethodHandle methodHandle;

    public DefaultFunctionBinder(MethodHandle methodHandle)
    {
        this.methodHandle = methodHandle;
    }

    public FunctionBinding bindFunction(long bindingId, String name, List<TypedByteCodeNode> arguments)
    {
        // extract constant arguments
        SortedMap<Integer, Object> constantArguments = new TreeMap<>(Ordering.natural().reverse());
        int argIndex = 0;
        for (TypedByteCodeNode argument : arguments) {
            ByteCodeNode node = argument.getNode();
            if (node instanceof Constant) {
                constantArguments.put(argIndex, ((Constant) node).getValue());
            }
            argIndex++;
        }

        // bind constant arguments
        MethodHandle methodHandle = this.methodHandle;
        arguments = new ArrayList<>(arguments);
        for (Entry<Integer, Object> entry : constantArguments.entrySet()) {
            methodHandle = MethodHandles.insertArguments(methodHandle, entry.getKey(), entry.getValue());
            arguments.remove((int) entry.getKey());
        }

        CallSite callSite = new ConstantCallSite(methodHandle);
        return new FunctionBinding(bindingId, name, callSite, arguments);
    }
}
