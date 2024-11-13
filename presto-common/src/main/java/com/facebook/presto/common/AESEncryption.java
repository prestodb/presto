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
package com.facebook.presto.common;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.KeyGenerator;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;

import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Optional;

public class AESEncryption
{
    private AESEncryption()
    {
    }
    // Method to encrypt the password using AES
    public static String encrypt(Optional<String> password, SecretKey secretKey)
    {
        if (password.isPresent()) {
            try {
                Cipher cipher = Cipher.getInstance("AES");
                cipher.init(Cipher.ENCRYPT_MODE, secretKey);
                byte[] encryptedBytes = cipher.doFinal(password.get().getBytes(StandardCharsets.UTF_8));
                return Base64.getEncoder().encodeToString(encryptedBytes);
            }
            catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException |
                     BadPaddingException | IllegalBlockSizeException e) {
                throw new RuntimeException("Error during key generation: " + e.getMessage(), e);
            }
        }
        else {
            throw new RuntimeException("No password provided for encryption.");
        }
    }

    // Method to decrypt the password using AES
    public static String decrypt(Optional<String> encryptedPassword, SecretKey secretKey)
    {
        if (encryptedPassword.isPresent()) {
            try {
                Cipher cipher = Cipher.getInstance("AES");
                cipher.init(Cipher.DECRYPT_MODE, secretKey);
                byte[] decodedBytes = Base64.getDecoder().decode(encryptedPassword.get());
                byte[] decryptedBytes = cipher.doFinal(decodedBytes);
                return new String(decryptedBytes, StandardCharsets.UTF_8);
            }
            catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException |
                     BadPaddingException | IllegalBlockSizeException e) {
                throw new RuntimeException("Error during key generation: " + e.getMessage(), e);
            }
        }
        else {
            throw new RuntimeException("No encrypted password provided for decryption.");
        }
    }

    // Method to generate a SecretKey for AES
    public static SecretKey generateSecretKey()
    {
        try {
            KeyGenerator keyGen = KeyGenerator.getInstance("AES");
            SecureRandom secureRandom = SecureRandom.getInstanceStrong();
            keyGen.init(128, secureRandom); // AES key size can be 128-bit, 192-bit, or 256-bit
            return keyGen.generateKey();
        }
        catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Error during key generation: " + e.getMessage(), e);
        }
    }
}
