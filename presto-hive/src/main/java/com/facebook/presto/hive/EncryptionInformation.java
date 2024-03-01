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

import com.facebook.presto.spi.PrestoException;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_INVALID_ENCRYPTION_METADATA;
import static java.util.Objects.hash;
import static java.util.Objects.requireNonNull;

/**
 * An object of {@link EncryptionInformation} will be passed to the readers and writers. The readers and writers
 * can decide which fields they want to read and how to use it.
 */
public class EncryptionInformation
{
    // Add a field for new file format encryption types
    private final Optional<DwrfEncryptionMetadata> dwrfEncryptionMetadata;

    // Only to be used by Jackson. Otherwise use {@link this#fromEncryptionMetadata}
    @JsonCreator
    public EncryptionInformation(@JsonProperty Optional<DwrfEncryptionMetadata> dwrfEncryptionMetadata)
    {
        this.dwrfEncryptionMetadata = requireNonNull(dwrfEncryptionMetadata, "dwrfEncryptionMetadata is null");
    }

    @JsonProperty
    public Optional<DwrfEncryptionMetadata> getDwrfEncryptionMetadata()
    {
        return dwrfEncryptionMetadata;
    }

    public static EncryptionInformation fromEncryptionMetadata(EncryptionMetadata encryptionMetadata)
    {
        requireNonNull(encryptionMetadata, "encryptionMetadata is null");

        if (encryptionMetadata instanceof DwrfEncryptionMetadata) {
            return new EncryptionInformation(Optional.of((DwrfEncryptionMetadata) encryptionMetadata));
        }

        throw new PrestoException(HIVE_INVALID_ENCRYPTION_METADATA, "Unknown encryptionMetadata type: " + encryptionMetadata.getClass());
    }

    @Override
    public int hashCode()
    {
        return hash(dwrfEncryptionMetadata);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this) {
            return true;
        }

        if (obj == null || !this.getClass().equals(obj.getClass())) {
            return false;
        }

        EncryptionInformation otherObj = (EncryptionInformation) obj;
        return Objects.equals(this.dwrfEncryptionMetadata, otherObj.dwrfEncryptionMetadata);
    }
}
