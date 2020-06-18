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
package com.facebook.presto.orc;

import io.airlift.slice.Slice;

public interface EncryptionLibrary
{
    Slice encryptKey(Slice keyMetadata, byte[] input, int offset, int length);
    Slice encryptData(Slice keyMetadata, byte[] input, int offset, int length);

    Slice decryptKey(Slice keyMetadata, byte[] input, int offset, int length);
    Slice decryptData(Slice keyMetadata, byte[] input, int offset, int length);

    int getMaxEncryptedLength(Slice keyMetadata, int unencryptedLength);
}
