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

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class DwrfEncryptionProvider
{
    public static final DwrfEncryptionProvider NO_ENCRYPTION = new DwrfEncryptionProvider(new UnsupportedEncryptionLibrary(), new UnsupportedEncryptionLibrary());

    private final EncryptionLibrary cryptoServiceLibrary;
    private final EncryptionLibrary unknownLibrary;

    public DwrfEncryptionProvider(EncryptionLibrary cryptoServiceLibrary, EncryptionLibrary unknownLibrary)
    {
        this.cryptoServiceLibrary = requireNonNull(cryptoServiceLibrary, "cryptoServiceLibrary is null");
        this.unknownLibrary = requireNonNull(unknownLibrary, "unknownLibrary is null");
    }

    public EncryptionLibrary getEncryptionLibrary(KeyProvider keyProvider)
    {
        switch (keyProvider) {
            case CRYPTO_SERVICE:
                if (cryptoServiceLibrary instanceof UnsupportedEncryptionLibrary) {
                    throw new UnsupportedOperationException("\"crypto_service\" encryption is not configured");
                }
                return cryptoServiceLibrary;
            case UNKNOWN:
                if (unknownLibrary instanceof UnsupportedEncryptionLibrary) {
                    throw new UnsupportedOperationException("\"unknown\" encryption is not configured");
                }
                return unknownLibrary;
            default:
                throw new IllegalArgumentException(format("Unknown KeyProvider: %s", keyProvider));
        }
    }
}
