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

import org.testng.annotations.Test;

import static com.facebook.presto.password.file.EncryptionUtil.getHashingAlgorithm;
import static com.facebook.presto.password.file.HashingAlgorithm.BCRYPT;
import static com.facebook.presto.password.file.HashingAlgorithm.PBKDF2;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestEncryptionUtil
{
    // check whether the correct hashing algorithm can be identified
    @Test
    public void testHashingAlgorithmBCrypt()
    {
        String password = "$2y$10$BqTb8hScP5DfcpmHo5PeyugxHz5Ky/qf3wrpD7SNm8sWuA3VlGqsa";
        assertEquals(getHashingAlgorithm(password), BCRYPT);
    }

    @Test
    public void testHashingAlgorithmPBKDF2()
    {
        String password = "1000:5b4240333032306164:f38d165fce8ce42f59d366139ef5d9e1ca1247f0e06e503ee1a611dd9ec40876bb5edb8409f5abe5504aab6628e70cfb3d3a18e99d70357d295002c3d0a308a0";
        assertEquals(getHashingAlgorithm(password), PBKDF2);
    }

    @Test
    public void testMinBCryptCost()
    {
        // BCrypt password created with cost of 7 --> "htpasswd -n -B -C 7 test"
        String password = "$2y$07$XxMSjoWesbX9s9LCD5Kp1OaFD/bcLUq0zoRCTsTNwjF6N/nwHVCVm";
        assertThatThrownBy(() -> getHashingAlgorithm(password))
                .isInstanceOf(HashedPasswordException.class)
                .hasMessage("Minimum cost of BCrypt password must be 8");
    }

    @Test
    public void testInvalidPasswordFormatPBKDF2()
    {
        // PBKDF2 password with iteration count of 100
        String password = "100:5b4240333032306164:f38d165fce8ce42f59d366139ef5d9e1ca1247f0e06e503ee1a611dd9ec40876bb5edb8409f5abe5504aab6628e70cfb3d3a18e99d70357d295002c3d0a308a0";
        assertThatThrownBy(() -> getHashingAlgorithm(password))
                .isInstanceOf(HashedPasswordException.class)
                .hasMessage("Minimum iterations of PBKDF2 password must be 1000");
    }
}
