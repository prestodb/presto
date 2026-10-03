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
package com.facebook.presto.spi.storage;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public interface TempStorage
{
    TempDataSink create(TempDataOperationContext context)
            throws IOException;

    default TempDataSink create(TempDataOperationContext context, TempStorageHandle handle, boolean createFile)
            throws IOException
    {
        throw new UnsupportedOperationException();
    }

    InputStream open(TempDataOperationContext context, TempStorageHandle handle)
            throws IOException;

    void remove(TempDataOperationContext context, TempStorageHandle handle)
            throws IOException;

    default boolean exists(TempDataOperationContext context, TempStorageHandle handle)
            throws IOException
    {
        throw new UnsupportedOperationException();
    }

    default boolean createIfNotExists(TempDataOperationContext context, TempStorageHandle handle, byte[] data)
            throws IOException
    {
        throw new UnsupportedOperationException();
    }

    TempStorageHandle getRootDirectoryHandle();

    byte[] serializeHandle(TempStorageHandle storageHandle);

    TempStorageHandle deserialize(byte[] serializedStorageHandle);

    List<StorageCapabilities> getStorageCapabilities();
}
