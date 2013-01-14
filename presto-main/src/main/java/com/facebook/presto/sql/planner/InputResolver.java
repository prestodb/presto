package com.facebook.presto.sql.planner;

import com.facebook.presto.operator.Input;

public interface InputResolver
{
    Object getValue(Input input);
}
