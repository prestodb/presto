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
package com.facebook.plugin.arrow;

import javax.annotation.concurrent.Immutable;

@Immutable
public class TestingConnectionProperties
{
    private final String database;
    private final String password;
    private final String host;
    private final Boolean ssl;
    private final String username;

    public TestingConnectionProperties(String database, String password, String host, Boolean ssl, String username)
    {
        this.database = database;
        this.password = password;
        this.host = host;
        this.ssl = ssl;
        this.username = username;
    }
}
