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
package com.facebook.presto.password.file;

import at.favre.lib.crypto.bcrypt.BCrypt;
import at.favre.lib.crypto.bcrypt.IllegalBCryptFormatException;
import com.google.common.base.Splitter;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.io.BaseEncoding.base16;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public final class EncryptionUtil
{
    private static final int BCRYPT_MIN_COST = 8;
    private static final int PBKDF2_MIN_ITERATIONS = 1000;

    private EncryptionUtil() {}

    public static int getBCryptCost(String password)
    {
        try {
            return BCrypt.Version.VERSION_2A.parser.parse(password.getBytes(UTF_8)).cost;
        }
        catch (IllegalBCryptFormatException e) {
            throw new HashedPasswordException("Invalid BCrypt password", e);
        }
    }

    public static int getPBKDF2Iterations(String password)
    {
        return PBKDF2Password.fromString(password).iterations();
    }

    public static boolean doesBCryptPasswordMatch(String inputPassword, String hashedPassword)
    {
        return BCrypt.verifyer().verify(inputPassword.toCharArray(), hashedPassword).verified;
    }

    public static boolean doesPBKDF2PasswordMatch(String inputPassword, String hashedPassword)
    {
        PBKDF2Password password = PBKDF2Password.fromString(hashedPassword);

        try {
            KeySpec spec = new PBEKeySpec(inputPassword.toCharArray(), password.salt(), password.iterations(), password.hash().length * 8);
            SecretKeyFactory key = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
            byte[] inputHash = key.generateSecret(spec).getEncoded();

            if (password.hash().length != inputHash.length) {
                throw new HashedPasswordException("PBKDF2 password input is malformed");
            }
            return MessageDigest.isEqual(password.hash(), inputHash);
        }
        catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
            throw new HashedPasswordException("Invalid PBKDF2 password", e);
        }
    }

    public static HashingAlgorithm getHashingAlgorithm(String password)
    {
        if (password.startsWith("$2y")) {
            if (getBCryptCost(password) < BCRYPT_MIN_COST) {
                throw new HashedPasswordException("Minimum cost of BCrypt password must be " + BCRYPT_MIN_COST);
            }
            return HashingAlgorithm.BCRYPT;
        }

        if (password.contains(":")) {
            if (getPBKDF2Iterations(password) < PBKDF2_MIN_ITERATIONS) {
                throw new HashedPasswordException("Minimum iterations of PBKDF2 password must be " + PBKDF2_MIN_ITERATIONS);
            }
            return HashingAlgorithm.PBKDF2;
        }

        throw new HashedPasswordException("Password hashing algorithm cannot be determined");
    }

    private static class PBKDF2Password
    {
        private final int iterations;
        private final byte[] salt;
        private final byte[] hash;

        private PBKDF2Password(int iterations, byte[] salt, byte[] hash)
        {
            this.iterations = iterations;
            this.salt = requireNonNull(salt, "salt is null");
            this.hash = requireNonNull(hash, "hash is null");
        }

        public int iterations()
        {
            return iterations;
        }

        public byte[] salt()
        {
            return salt;
        }

        public byte[] hash()
        {
            return hash;
        }

        public static PBKDF2Password fromString(String password)
        {
            try {
                List<String> parts = Splitter.on(":").splitToList(password);
                checkArgument(parts.size() == 3, "wrong part count");

                int iterations = Integer.parseInt(parts.get(0));
                byte[] salt = base16().lowerCase().decode(parts.get(1));
                byte[] hash = base16().lowerCase().decode(parts.get(2));

                return new PBKDF2Password(iterations, salt, hash);
            }
            catch (IllegalArgumentException e) {
                throw new HashedPasswordException("Invalid PBKDF2 password");
            }
        }
    }
}
