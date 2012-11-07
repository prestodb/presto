package com.facebook.presto.metadata;

import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.tuple.TupleInfo;

import java.util.List;

public class FunctionInfo
{
    private final QualifiedName name;
    private final boolean isAggregate;
    private final TupleInfo.Type returnType;
    private final List<TupleInfo.Type> argumentTypes;

    public FunctionInfo(QualifiedName name, boolean aggregate, TupleInfo.Type returnType, List<TupleInfo.Type> argumentTypes)
    {
        this.name = name;
        isAggregate = aggregate;
        this.returnType = returnType;
        this.argumentTypes = argumentTypes;
    }

    public QualifiedName getName()
    {
        return name;
    }

    public boolean isAggregate()
    {
        return isAggregate;
    }

    public TupleInfo.Type getReturnType()
    {
        return returnType;
    }

    public List<TupleInfo.Type> getArgumentTypes()
    {
        return argumentTypes;
    }
}
