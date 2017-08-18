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
package com.facebook.presto.execution.buffer;

import com.facebook.presto.spi.PrestoException;

import org.bouncycastle.crypto.engines.AESEngine;
import org.bouncycastle.crypto.modes.CBCBlockCipher;
import org.bouncycastle.crypto.paddings.PKCS7Padding;
import org.bouncycastle.crypto.paddings.PaddedBufferedBlockCipher;
import org.bouncycastle.crypto.params.KeyParameter;
import org.bouncycastle.crypto.params.ParametersWithIV;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

import java.security.SecureRandom;
import java.util.Arrays;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

public class AES256Encryptor
        implements Encryptor
{
    private static final String ENCRYPTION_ALGORITHM = "AES";
    private static final int KEY_SIZE = 256;

    private final PaddedBufferedBlockCipher pbbc;
    private final KeyParameter key;
    private final int blockSize;

    public AES256Encryptor()
    {
        this.pbbc = new PaddedBufferedBlockCipher(new CBCBlockCipher(new AESEngine()), new PKCS7Padding());
        this.key = new KeyParameter(getKey());
        this.blockSize = pbbc.getBlockSize();
    }

    public byte[] encrypt(byte[] data)
    {
        try {
            return cipherData(data, true);
        }
        catch (Exception e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Exception thrown while encrypting a spill page", e);
        }
    }

    public byte[] decrypt(byte[] data)
    {
        try {
            return cipherData(data, false);
        }
        catch (Exception e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Exception thrown while decrypting a spill page", e);
        }
    }

    private byte[] cipherData(byte[] data, boolean encrypt) throws Exception
    {
        int inputOffset = 0;
        int inputLength = data.length;
        int outputOffset = 0;

        byte[] iv = new byte[blockSize];
        if (encrypt) {
            SecureRandom random = new SecureRandom();
            random.nextBytes(iv);
            outputOffset += blockSize;
        }
        else {
            System.arraycopy(data, 0, iv, 0, blockSize);
            inputOffset += blockSize;
            inputLength -= blockSize;
        }

        pbbc.init(encrypt, new ParametersWithIV(key, iv));
        byte[] output = new byte[pbbc.getOutputSize(inputLength) + outputOffset];

        if (encrypt) {
            System.arraycopy(iv, 0, output, 0, blockSize);
        }

        int outputLength = outputOffset + pbbc.processBytes(data, inputOffset, inputLength, output, outputOffset);
        outputLength += pbbc.doFinal(output, outputLength);

        return Arrays.copyOf(output, outputLength);
    }

    private byte[] getKey()
    {
        try {
            KeyGenerator kg = KeyGenerator.getInstance(ENCRYPTION_ALGORITHM);
            kg.init(KEY_SIZE);
            SecretKey sk = kg.generateKey();

            return sk.getEncoded();
        }
        catch (Exception e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Exception thrown while creating spill encryptor", e);
        }
    }
}
