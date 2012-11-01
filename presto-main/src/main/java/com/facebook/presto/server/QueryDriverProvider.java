/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

public interface QueryDriverProvider {
    QueryDriver create(QueryState queryState);
}
