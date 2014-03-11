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
package com.facebook.presto.hive;

import com.facebook.presto.spi.ErrorCode;

public enum HiveErrorCode
        implements ErrorCode
{
    // Connectors can use error codes starting at EXTERNAL
    HIVE_METASTORE_ERROR(0x0100_0000),
    HIVE_CURSOR_ERROR(0x0100_0001),
    HIVE_TABLE_OFFLINE(0x0100_0002);

    private final int code;

    HiveErrorCode(int code)
    {
        this.code = code;
    }

    @Override
    public int getCode()
    {
        return code;
    }

    @Override
    public String getName()
    {
        return name();
    }
}
