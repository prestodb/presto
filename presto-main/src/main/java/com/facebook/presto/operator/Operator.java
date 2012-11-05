/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator;

public interface Operator extends Iterable<Page> {
    int getChannelCount();
}
