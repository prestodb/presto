/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.spi;

import java.util.List;

public interface RecordSet
{
    List<ColumnType> getColumnTypes();
    RecordCursor cursor();
}
