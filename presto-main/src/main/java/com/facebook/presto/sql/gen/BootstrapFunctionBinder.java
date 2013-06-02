package com.facebook.presto.sql.gen;

import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.sql.gen.ExpressionCompiler.TypedByteCodeNode;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import io.airlift.slice.Slice;

import java.lang.invoke.CallSite;
import java.lang.invoke.MethodType;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class BootstrapFunctionBinder
{
    private static final AtomicLong NEXT_BINDING_ID = new AtomicLong();

    private final Metadata metadata;

    private final ConcurrentMap<Long, FunctionBinding> functionBindings = new ConcurrentHashMap<>();

    public BootstrapFunctionBinder(Metadata metadata)
    {
        this.metadata = checkNotNull(metadata, "metadata is null");
    }

    public FunctionBinding bindFunction(QualifiedName name, List<TypedByteCodeNode> arguments)
    {
        List<Type> argumentTypes = Lists.transform(arguments, toTupleType());
        FunctionInfo function = metadata.getFunction(name, argumentTypes);
        checkArgument(function != null, "Unknown function %s%s", name, argumentTypes);

        FunctionBinder defaultFunctionBinder = new DefaultFunctionBinder(function.getScalarFunction());
        FunctionBinding functionBinding = bindFunction(name.toString(), arguments, defaultFunctionBinder);

        return functionBinding;
    }

    public FunctionBinding bindFunction(String name, List<TypedByteCodeNode> arguments, FunctionBinder defaultFunctionBinder)
    {
        // perform binding
        FunctionBinding functionBinding = defaultFunctionBinder.bindFunction(NEXT_BINDING_ID.getAndIncrement(), name, arguments);

        // record binding
        functionBindings.put(functionBinding.getBindingId(), functionBinding);

        return functionBinding;
    }

    public CallSite bootstrap(String name, MethodType type, long bindingId)
    {
        FunctionBinding functionBinding = functionBindings.get(bindingId);
        checkArgument(functionBinding != null, "Binding %s for function %s%s not found", bindingId, name, type.parameterList());

        return functionBinding.getCallSite();
    }

    public static Function<TypedByteCodeNode, Type> toTupleType()
    {
        return new Function<TypedByteCodeNode, Type>()
        {
            @Override
            public Type apply(TypedByteCodeNode node)
            {
                Class<?> type = node.getType();
                if (type == boolean.class) {
                    return Type.BOOLEAN;
                }
                if (type == long.class) {
                    return Type.LONG;
                }
                if (type == double.class) {
                    return Type.DOUBLE;
                }
                if (type == String.class) {
                    return Type.STRING;
                }
                if (type == Slice.class) {
                    return Type.STRING;
                }
                throw new UnsupportedOperationException("Unsupported function type " + type);
            }
        };
    }
}
