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

import static com.google.common.base.Preconditions.checkArgument;

public class DefaultFunctionBinder
        implements FunctionBinder
{
    private final MethodHandle methodHandle;
    private final boolean nullable;

    public DefaultFunctionBinder(MethodHandle methodHandle, boolean nullable)
    {
        this.methodHandle = methodHandle;
        this.nullable = nullable;
    }

    public FunctionBinding bindFunction(long bindingId, String name, List<TypedByteCodeNode> arguments)
    {
        return bindConstantArguments(bindingId, name, arguments, this.methodHandle, nullable);
    }

    public static FunctionBinding bindConstantArguments(long bindingId, String name, List<TypedByteCodeNode> arguments, MethodHandle methodHandle, boolean nullable)
    {
        // extract constant arguments
        SortedMap<Integer, Object> constantArguments = new TreeMap<>(Ordering.natural().reverse());
        int argIndex = 0;
        for (TypedByteCodeNode argument : arguments) {
            ByteCodeNode node = argument.getNode();
            if (node instanceof Constant) {
                Object value = ((Constant) node).getValue();
                if (argument.getType() == boolean.class) {
                    checkArgument(value instanceof Integer, "boolean should be represented as an integer");
                    value = (value != 0);
                }
                constantArguments.put(argIndex, value);
            }
            argIndex++;
        }

        // bind constant arguments
        arguments = new ArrayList<>(arguments);
        for (Entry<Integer, Object> entry : constantArguments.entrySet()) {
            methodHandle = MethodHandles.insertArguments(methodHandle, entry.getKey(), entry.getValue());
            arguments.remove((int) entry.getKey());
        }

        CallSite callSite = new ConstantCallSite(methodHandle);
        return new FunctionBinding(bindingId, name, callSite, arguments, nullable);
    }
}
