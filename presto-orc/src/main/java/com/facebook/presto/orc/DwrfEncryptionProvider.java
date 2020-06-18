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

import java.util.Optional;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class DwrfEncryptionProvider
{
    public static final DwrfEncryptionProvider NO_ENCRYPTION = new DwrfEncryptionProvider(Optional.empty(), Optional.empty());

    private final Optional<EncryptionLibrary> cryptoServiceLibrary;
    private final Optional<EncryptionLibrary> unknownLibrary;

    public DwrfEncryptionProvider(Optional<EncryptionLibrary> cryptoServiceLibrary, Optional<EncryptionLibrary> unknownLibrary)
    {
        this.cryptoServiceLibrary = requireNonNull(cryptoServiceLibrary, "cryptoServiceLibrary is null");
        this.unknownLibrary = requireNonNull(unknownLibrary, "unknownLibrary is null");
    }

    public EncryptionLibrary getEncryptionLibrary(KeyProvider keyProvider)
    {
        switch (keyProvider) {
            case CRYPTO_SERVICE:
                if (!cryptoServiceLibrary.isPresent()) {
                    throw new UnsupportedOperationException("\"crypto_service\" encryption is not configured");
                }
                return cryptoServiceLibrary.get();
            case UNKNOWN:
                if (!unknownLibrary.isPresent()) {
                    throw new UnsupportedOperationException("\"unknown\" encryption is not configured");
                }
                return unknownLibrary.get();
            default:
                throw new IllegalArgumentException(format("Unknown KeyProvider: %s", keyProvider));
        }
    }
}
