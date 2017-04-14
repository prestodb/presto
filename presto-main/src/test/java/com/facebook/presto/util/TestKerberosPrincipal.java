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
package com.facebook.presto.util;

import org.testng.annotations.Test;

import java.util.Optional;

import static org.testng.Assert.assertEquals;

public class TestKerberosPrincipal
{
    @Test
    public void testPrincipalParsing()
            throws Exception
    {
        assertEquals(KerberosPrincipal.valueOf("user"), new KerberosPrincipal("user", Optional.empty(), Optional.empty()));
        assertEquals(KerberosPrincipal.valueOf("user@REALM.COM"), new KerberosPrincipal("user", Optional.empty(), Optional.of("REALM.COM")));
        assertEquals(KerberosPrincipal.valueOf("user/hostname@REALM.COM"), new KerberosPrincipal("user", Optional.of("hostname"), Optional.of("REALM.COM")));
        assertEquals(KerberosPrincipal.valueOf("user\\@host.com@REALM.COM"), new KerberosPrincipal("user\\@host.com", Optional.empty(), Optional.of("REALM.COM")));
        assertEquals(KerberosPrincipal.valueOf("user/_HOST@REALM.COM"), new KerberosPrincipal("user", Optional.of("_HOST"), Optional.of("REALM.COM")));
        assertEquals(KerberosPrincipal.valueOf("user/part1/part2/_HOST@REALM.COM"), new KerberosPrincipal("user/part1/part2", Optional.of("_HOST"), Optional.of("REALM.COM")));
        assertEquals(KerberosPrincipal.valueOf("user\\/part1\\/part2\\/_HOST@REALM.COM"), new KerberosPrincipal("user\\/part1\\/part2\\/_HOST", Optional.empty(), Optional.of("REALM.COM")));
        assertEquals(KerberosPrincipal.valueOf("user\\/part1\\/part2\\/_HOST\\@REALM.COM"), new KerberosPrincipal("user\\/part1\\/part2\\/_HOST\\@REALM.COM", Optional.empty(), Optional.empty()));
    }

    @Test
    public void testSubstituteHostnamePlaceholder()
            throws Exception
    {
        assertEquals(
                KerberosPrincipal.valueOf("user").substituteHostnamePlaceholder("localhost"),
                new KerberosPrincipal("user", Optional.empty(), Optional.empty()));
        assertEquals(
                KerberosPrincipal.valueOf("user/_HOST").substituteHostnamePlaceholder("localhost"),
                new KerberosPrincipal("user", Optional.of("localhost"), Optional.empty()));
        assertEquals(
                KerberosPrincipal.valueOf("user/_HOST@REALM.COM").substituteHostnamePlaceholder("localhost"),
                new KerberosPrincipal("user", Optional.of("localhost"), Optional.of("REALM.COM")));
        assertEquals(
                KerberosPrincipal.valueOf("user@REALM.COM").substituteHostnamePlaceholder("localhost"),
                new KerberosPrincipal("user", Optional.empty(), Optional.of("REALM.COM")));
    }

    @Test
    public void testToString()
            throws Exception
    {
        assertEquals(new KerberosPrincipal("user", Optional.empty(), Optional.empty()).toString(), "user");
        assertEquals(new KerberosPrincipal("user", Optional.of("host"), Optional.empty()).toString(), "user/host");
        assertEquals(new KerberosPrincipal("user", Optional.of("host"), Optional.of("REALM.COM")).toString(), "user/host@REALM.COM");
    }
}
