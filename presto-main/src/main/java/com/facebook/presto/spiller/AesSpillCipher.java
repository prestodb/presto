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
package com.facebook.presto.spiller;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.spiller.SpillCipher;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;

import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.security.NoSuchAlgorithmException;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.lang.String.format;

final class AesSpillCipher
        implements SpillCipher
{
    //  256-bit AES CBC mode
    private static final String CIPHER_NAME = "AES/CBC/PKCS5Padding";
    private static final int KEY_BITS = 256;

    private SecretKey key;
    private final int ivBytes;

    AesSpillCipher()
    {
        this.key = generateNewSecretKey();
        this.ivBytes = createEncryptCipher().getIV().length;
    }

    @Override
    public ByteBuffer decrypt(ByteBuffer encryptedData)
    {
        int length = encryptedData.remaining();
        if (length < ivBytes) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, format("Encrypted data size is too small: %s. It must be at least the size of the initialization vector: %s.", length, ivBytes));
        }
        byte[] iv = new byte[ivBytes];
        encryptedData.get(iv);
        Cipher cipher = createDecryptCipher(new IvParameterSpec(iv));
        ByteBuffer output = ByteBuffer.allocate(cipher.getOutputSize(encryptedData.remaining()));
        try {
            cipher.doFinal(encryptedData, output);
            output.flip();
            return output;
        }
        catch (GeneralSecurityException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Cannot decrypt previously encrypted data: " + e.getMessage(), e);
        }
    }

    @Override
    public byte[] decrypt(byte[] encryptedData)
    {
        return unwrapOrCopyToByteArray(decrypt(ByteBuffer.wrap(encryptedData)));
    }

    @Override
    public ByteBuffer encrypt(ByteBuffer data)
    {
        Cipher cipher = createEncryptCipher();
        byte[] iv = cipher.getIV();
        ByteBuffer output = ByteBuffer
                .allocate(iv.length + cipher.getOutputSize(data.remaining()))
                .put(iv);
        try {
            cipher.doFinal(data, output);
            output.flip();
            return output;
        }
        catch (GeneralSecurityException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to encrypt data: " + e.getMessage(), e);
        }
    }

    @Override
    public byte[] encrypt(byte[] data)
    {
        return unwrapOrCopyToByteArray(encrypt(ByteBuffer.wrap(data)));
    }

    /**
     * Setting the {@link AesSpillCipher#key} to null allows the key to be reclaimed by GC at the time of
     * destruction, even if the {@link AesSpillCipher} itself is still reachable for longer. When the key
     * is null, subsequent encrypt / decrypt operations will fail and can prevent accidental use beyond the
     * intended lifespan of the {@link AesSpillCipher}.
     */
    @Override
    public void destroy()
    {
        this.key = null;
    }

    @Override
    public boolean isDestroyed()
    {
        return key == null;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this).add("isDestroyed", isDestroyed()).toString();
    }

    private SecretKey verifyKeyNotDestroyed()
    {
        SecretKey key = this.key;
        if (key == null) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Spill cipher already destroyed");
        }
        return key;
    }

    private Cipher createEncryptCipher()
    {
        SecretKey key = verifyKeyNotDestroyed();
        Cipher cipher = createUninitializedCipher();
        try {
            cipher.init(Cipher.ENCRYPT_MODE, key);
            return cipher;
        }
        catch (GeneralSecurityException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to initialize spill cipher for encryption: " + e.getMessage(), e);
        }
    }

    private Cipher createDecryptCipher(IvParameterSpec iv)
    {
        SecretKey key = verifyKeyNotDestroyed();
        Cipher cipher = createUninitializedCipher();
        try {
            cipher.init(Cipher.DECRYPT_MODE, key, iv);
            return cipher;
        }
        catch (GeneralSecurityException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to initialize spill cipher for decryption: " + e.getMessage(), e);
        }
    }

    /**
     * Unwraps {@link java.nio.ByteBuffer} instances backed by arrays that are exact matches in size to
     * their buffer. This is expected during wrapped encrypt / decrypt buffers and those buffers are
     * not externally visible (and therefore safe to unwrap without copies)
     */
    private static byte[] unwrapOrCopyToByteArray(ByteBuffer buffer)
    {
        if (buffer.hasArray() && buffer.arrayOffset() == 0 && buffer.capacity() == buffer.remaining()) {
            return buffer.array();
        }
        byte[] copy = new byte[buffer.remaining()];
        buffer.get(copy);
        return copy;
    }

    private static Cipher createUninitializedCipher()
    {
        try {
            return Cipher.getInstance(CIPHER_NAME);
        }
        catch (GeneralSecurityException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to create spill cipher: " + e.getMessage(), e);
        }
    }

    private static SecretKey generateNewSecretKey()
    {
        try {
            KeyGenerator keyGenerator = KeyGenerator.getInstance("AES");
            keyGenerator.init(KEY_BITS);
            return keyGenerator.generateKey();
        }
        catch (NoSuchAlgorithmException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to generate new secret key: " + e.getMessage(), e);
        }
    }
}
