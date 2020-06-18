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

import com.facebook.presto.orc.DwrfEncryptionProvider;
import com.facebook.presto.orc.EncryptionLibrary;
import com.facebook.presto.orc.UnsupportedEncryptionLibrary;
import com.google.inject.BindingAnnotation;
import com.google.inject.Inject;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Objects.requireNonNull;

/**
 * this class is used to make DwrfEncryptionProvider injectable for hive
 */
public class HiveDwrfEncryptionProvider
{
    public static final HiveDwrfEncryptionProvider NO_ENCRYPTION = new HiveDwrfEncryptionProvider(new UnsupportedEncryptionLibrary(), new UnsupportedEncryptionLibrary());

    private final EncryptionLibrary cryptoServiceLibrary;
    private final EncryptionLibrary unknownLibrary;

    @Inject
    public HiveDwrfEncryptionProvider(@ForCryptoService EncryptionLibrary cryptoServiceLibrary, @ForUnknown EncryptionLibrary unknownLibrary)
    {
        this.cryptoServiceLibrary = requireNonNull(cryptoServiceLibrary, "cryptoServiceLibrary is null");
        this.unknownLibrary = requireNonNull(unknownLibrary, "unknownLibrary is null");
    }

    public DwrfEncryptionProvider toDwrfEncryptionProvider()
    {
        return new DwrfEncryptionProvider(cryptoServiceLibrary, unknownLibrary);
    }

    @BindingAnnotation
    @Target(PARAMETER)
    @Retention(RUNTIME)
    public @interface ForCryptoService {}

    @BindingAnnotation
    @Target(PARAMETER)
    @Retention(RUNTIME)
    public @interface ForUnknown {}
}
