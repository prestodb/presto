/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.ingest;

import com.facebook.presto.operator.OperatorStats;

public interface RecordSet
{
    RecordCursor cursor(OperatorStats operatorStats);
}
