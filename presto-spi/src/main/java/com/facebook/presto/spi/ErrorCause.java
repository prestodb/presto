/*
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
package com.facebook.presto.spi;

import com.facebook.drift.annotations.ThriftEnum;
import com.facebook.drift.annotations.ThriftEnumValue;
import com.facebook.presto.common.experimental.auto_gen.ThriftErrorCause;

@ThriftEnum
public enum ErrorCause
{
    UNKNOWN(0),
    LOW_PARTITION_COUNT(1),
    EXCEEDS_BROADCAST_MEMORY_LIMIT(2);

    private final int code;

    public static ErrorCause createErrorCause(ThriftErrorCause thriftErrorCause)
    {
        return ErrorCause.valueOf(thriftErrorCause.name());
    }

    public ThriftErrorCause toThrift()
    {
        return ThriftErrorCause.valueOf(name());
    }

    ErrorCause(int code)
    {
        this.code = code;
    }

    @ThriftEnumValue
    public int getCode()
    {
        return code;
    }
}
