/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.noperator;

public interface Operator extends Iterable<Page> {
    int getChannelCount();
}
