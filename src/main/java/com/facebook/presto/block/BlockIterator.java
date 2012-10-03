/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.block;

import java.util.Iterator;

public interface BlockIterator<T extends TupleStream> extends Iterator<T> {
    boolean mustYield();
    boolean canAdvance();
}
