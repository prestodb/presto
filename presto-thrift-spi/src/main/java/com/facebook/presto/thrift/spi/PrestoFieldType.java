/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.thrift.spi;

import com.facebook.swift.codec.ThriftStruct;

@ThriftStruct
public enum PrestoFieldType
{
    LONG,
    DOUBLE,
    STRING
}
