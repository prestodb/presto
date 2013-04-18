package com.facebook.presto.sql.planner;

/**
 * Receives feedback from the execution stage. A worker node can send output
 * back to the coordinator that is associated with a plan node.
 */
public interface OutputReceiver
{
    /**
     * Called with an object returned by a worker. An object is received at least once and can be
     * received multiple times.
     */
    void updateOutput(Object value);
}
