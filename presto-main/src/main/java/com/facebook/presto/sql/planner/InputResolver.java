package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.tree.Input;

public interface InputResolver
{
    Object getValue(Input input);
}
