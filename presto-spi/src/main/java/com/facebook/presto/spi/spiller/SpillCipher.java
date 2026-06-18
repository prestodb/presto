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
package com.facebook.presto.spi.spiller;

import java.nio.ByteBuffer;

public interface SpillCipher
{
    /**
     * Encrypts the source data
     */
    byte[] encrypt(byte[] data);

    /**
     * Encrypts the contents of the input ByteBuffer into a new ByteBuffer instance. The returned instance will
     * be at position 0 and ready to read.
     */
    ByteBuffer encrypt(ByteBuffer data);

    /**
     * Decrypts encrypted data
     */
    byte[] decrypt(byte[] encryptedData);

    /**
     * Decrypts the given {@link ByteBuffer} contents and returns them in a new {@link ByteBuffer}. The returned
     * instance will be at position 0 and ready to read.
     */
    ByteBuffer decrypt(ByteBuffer encryptedData);

    /**
     * Destroy encryption key preventing future use. Implementations should allow this to be called multiple times
     * without failing.
     */
    void destroy();

    /**
     * Indicates whether {@link SpillCipher#destroy()} has already been called to destroy the cipher
     */
    boolean isDestroyed();
}
