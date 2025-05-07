/*
 * Copyright (C) 2012 Facebook, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto;

import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.facebook.presto.common.transaction.TransactionId;

import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;

import static com.facebook.drift.annotations.ThriftField.Requiredness.OPTIONAL;

@ThriftStruct("OptionalField")
public class OptionalField
{
    @ThriftField(1)
    public Optional<String> optionalString;

    @ThriftField(value = 2, requiredness = OPTIONAL)
    public Optional<Long> optionalLong;

    @ThriftField(value = 3, requiredness = OPTIONAL)
    public Integer optionalInteger;

    @ThriftField(value = 4, requiredness = OPTIONAL)
    public OptionalInt primitiveOptionalInt;

    @ThriftField(value = 5, requiredness = OPTIONAL)
    public OptionalLong primitiveOptionalLong;

    @ThriftField(value = 6)
    public OptionalDouble primitiveOptionalDouble;

    @ThriftField(value = 7)
    public Optional<TransactionId> transactionId;
}
