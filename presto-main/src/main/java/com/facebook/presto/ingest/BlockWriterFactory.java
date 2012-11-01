/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.ingest;

import com.facebook.presto.serde.BlocksWriter;

public interface BlockWriterFactory
{
    BlocksWriter create();
}
