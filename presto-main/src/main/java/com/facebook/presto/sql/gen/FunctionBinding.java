package com.facebook.presto.sql.gen;

import com.google.common.base.Objects;

import java.lang.invoke.CallSite;
import java.util.List;

public class FunctionBinding
{
    private final long bindingId;
    private final String name;
    private final CallSite callSite;
    private final List<TypedByteCodeNode> arguments;
    private final boolean nullable;

    public FunctionBinding(long bindingId, String name, CallSite callSite, List<TypedByteCodeNode> arguments, boolean nullable)
    {
        this.bindingId = bindingId;
        this.name = name;
        this.callSite = callSite;
        this.arguments = arguments;
        this.nullable = nullable;
    }

    public long getBindingId()
    {
        return bindingId;
    }

    public String getName()
    {
        return name;
    }

    public CallSite getCallSite()
    {
        return callSite;
    }

    public List<TypedByteCodeNode> getArguments()
    {
        return arguments;
    }

    public boolean isNullable()
    {
        return nullable;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(bindingId);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final FunctionBinding other = (FunctionBinding) obj;
        return Objects.equal(this.bindingId, other.bindingId);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("bindingId", bindingId)
                .add("name", name)
                .add("callSite", callSite)
                .add("arguments", arguments)
                .add("nullable", nullable)
                .toString();
    }
}
