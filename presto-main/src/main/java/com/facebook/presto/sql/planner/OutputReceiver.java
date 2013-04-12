package com.facebook.presto.sql.planner;

public interface OutputReceiver
{
    void receive(Object value); // lame
}
