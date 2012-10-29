/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.nblock;

import com.facebook.presto.TupleInfo;

public interface Blocks extends Iterable<Block> {
    /**
     * Gets the type of all tuples in this cursor
     */
    TupleInfo getTupleInfo();
}
