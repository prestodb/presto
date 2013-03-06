/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import org.joda.time.DateTime;

public class AbandonedException
    extends RuntimeException
{
    public AbandonedException(String name, DateTime lastHeartBeat, DateTime now)
    {
        super(String.format("%s has not been accessed since %s: currentTime %s", name, lastHeartBeat, now));
    }
}
