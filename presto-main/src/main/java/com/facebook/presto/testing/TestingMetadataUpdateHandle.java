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
package com.facebook.presto.testing;

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.facebook.presto.spi.ConnectorMetadataUpdateHandle;

@ThriftStruct
public class TestingMetadataUpdateHandle
        implements ConnectorMetadataUpdateHandle
{
    private final int val;

    @ThriftConstructor
    public TestingMetadataUpdateHandle(int val)
    {
        this.val = val;
    }

    @ThriftField(1)
    public int getVal()
    {
        return val;
    }
}
