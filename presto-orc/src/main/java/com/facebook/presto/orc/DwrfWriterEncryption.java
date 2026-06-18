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

import com.facebook.presto.orc.metadata.KeyProvider;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class DwrfWriterEncryption
{
    private final KeyProvider keyProvider;
    private final List<WriterEncryptionGroup> writerEncryptionGroups;

    public DwrfWriterEncryption(KeyProvider keyProvider, List<WriterEncryptionGroup> writerEncryptionGroups)
    {
        this.keyProvider = requireNonNull(keyProvider, "keyProvider is null");
        this.writerEncryptionGroups = requireNonNull(writerEncryptionGroups, "writerEncryptionGroups is null");
    }

    public List<WriterEncryptionGroup> getWriterEncryptionGroups()
    {
        return writerEncryptionGroups;
    }

    public KeyProvider getKeyProvider()
    {
        return keyProvider;
    }
}
