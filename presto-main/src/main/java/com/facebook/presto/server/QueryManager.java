/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.block.uncompressed.UncompressedBlock;

import java.util.List;

public interface QueryManager {
    String createQuery(String query);

    List<UncompressedBlock> getQueryResults(String queryId, int maxBlockCount)
            throws InterruptedException;

    void destroyQuery(String queryId);
}
