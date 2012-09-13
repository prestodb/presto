/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.ingest;

import java.io.IOException;

public interface ColumnProcessor
{
    boolean processPositions(long end)
            throws IOException;

    void finish()
            throws IOException;
}
