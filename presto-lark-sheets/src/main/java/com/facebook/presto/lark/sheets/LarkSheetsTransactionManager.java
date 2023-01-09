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
package com.facebook.presto.lark.sheets;

import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import java.util.concurrent.ConcurrentHashMap;

public class LarkSheetsTransactionManager
{
    private final ConcurrentHashMap<ConnectorTransactionHandle, LarkSheetsMetadata> metadataMap = new ConcurrentHashMap<>();

    public void put(ConnectorTransactionHandle handle, LarkSheetsMetadata metadata)
    {
        metadataMap.put(handle, metadata);
    }

    public LarkSheetsMetadata get(ConnectorTransactionHandle handle)
    {
        return metadataMap.get(handle);
    }

    public void remove(ConnectorTransactionHandle handle)
    {
        metadataMap.remove(handle);
    }
}
