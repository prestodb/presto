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

import static java.util.Objects.requireNonNull;

public class DecryptingDecompressor
        implements OrcDecompressor
{
    private final DwrfDataEncryptor decryptor;
    private final OrcDecompressor decompressor;

    public DecryptingDecompressor(DwrfDataEncryptor decryptor, OrcDecompressor decompressor)
    {
        this.decompressor = requireNonNull(decompressor, "decompressor is null");
        this.decryptor = requireNonNull(decryptor, "decryptor is null");
    }

    @Override
    public int decompress(byte[] input, int offset, int length, OutputBuffer output)
            throws OrcCorruptionException
    {
        Slice decryptedInput = decryptor.decrypt(input, offset, length);
        return decompressor.decompress(decryptedInput.byteArray(), decryptedInput.byteArrayOffset(), decryptedInput.length(), output);
    }
}
