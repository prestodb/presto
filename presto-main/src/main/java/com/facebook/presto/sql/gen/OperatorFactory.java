package com.facebook.presto.sql.gen;

import com.facebook.presto.operator.Operator;
import com.facebook.presto.sql.analyzer.Session;

public interface OperatorFactory
{
    Operator createOperator(Operator source, Session session);
}
