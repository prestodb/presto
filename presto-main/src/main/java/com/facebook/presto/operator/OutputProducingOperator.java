package com.facebook.presto.operator;

import java.util.Set;

public interface OutputProducingOperator<T>
{
    Set<T> getOutput();
}
