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
package com.facebook.presto.sql.query;

import com.facebook.presto.Session;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.sql.SqlPath;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public class TestSessionFunctions
{
    @Test
    public void testCurrentUser()
    {
        Session session = testSessionBuilder().setIdentity(new Identity("test_current_user", Optional.empty())).build();
        try (QueryAssertions queryAssertions = new QueryAssertions(session)) {
            queryAssertions.assertQuery("SELECT CURRENT_USER", "SELECT CAST('" + session.getUser() + "' AS VARCHAR)");
        }
    }

    @Test
    public void testCurrentPath()
    {
        Session session = testSessionBuilder()
                .setPath(new SqlPath(Optional.of("testPath")))
                .build();

        try (QueryAssertions queryAssertions = new QueryAssertions(session)) {
            queryAssertions.assertQuery("SELECT CURRENT_PATH", "SELECT CAST('" + session.getPath().toString() + "' AS VARCHAR)");
        }

        Session emptyPathSession = testSessionBuilder()
                .setPath(new SqlPath(Optional.of("\"\"")))
                .build();

        try (QueryAssertions queryAssertions = new QueryAssertions(emptyPathSession)) {
            queryAssertions.assertQuery("SELECT CURRENT_PATH", "SELECT CAST('" + emptyPathSession.getPath().toString() + "' AS VARCHAR)");
        }
    }
}
