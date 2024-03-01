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

import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestPasswordStore
{
    private static final String BCRYPT_PASSWORD = "$2y$10$BqTb8hScP5DfcpmHo5PeyugxHz5Ky/qf3wrpD7SNm8sWuA3VlGqsa";
    private static final String PBKDF2_PASSWORD = "1000:5b4240333032306164:f38d165fce8ce42f59d366139ef5d9e1ca1247f0e06e503ee1a611dd9ec40876bb5edb8409f5abe5504aab6628e70cfb3d3a18e99d70357d295002c3d0a308a0";

    @Test
    public void testAuthenticate()
    {
        PasswordStore store = createStore("userbcrypt:" + BCRYPT_PASSWORD, "userpbkdf2:" + PBKDF2_PASSWORD);

        assertTrue(store.authenticate("userbcrypt", "user123"));
        assertFalse(store.authenticate("userbcrypt", "user999"));
        assertFalse(store.authenticate("userbcrypt", "password"));

        assertTrue(store.authenticate("userpbkdf2", "password"));
        assertFalse(store.authenticate("userpbkdf2", "password999"));
        assertFalse(store.authenticate("userpbkdf2", "user123"));

        assertFalse(store.authenticate("baduser", "user123"));
        assertFalse(store.authenticate("baduser", "password"));
    }

    @Test
    public void testEmptyFile()
    {
        createStore();
    }

    @Test
    public void testInvalidFile()
    {
        assertThatThrownBy(() -> createStore("", "junk"))
                .hasMessage("Error in password file line 2: Expected two parts for user and password");

        assertThatThrownBy(() -> createStore("abc:" + BCRYPT_PASSWORD, "xyz:" + BCRYPT_PASSWORD, "abc:" + PBKDF2_PASSWORD))
                .hasMessage("Error in password file line 3: Duplicate user: abc");

        assertThatThrownBy(() -> createStore("x:x"))
                .hasMessage("Error in password file line 1: Password hashing algorithm cannot be determined");

        assertThatThrownBy(() -> createStore("x:$2y$xxx"))
                .hasMessage("Error in password file line 1: Invalid BCrypt password");

        assertThatThrownBy(() -> createStore("x:x:x"))
                .hasMessage("Error in password file line 1: Invalid PBKDF2 password");
    }

    private static PasswordStore createStore(String... lines)
    {
        return new PasswordStore(ImmutableList.copyOf(lines), 1000);
    }
}
