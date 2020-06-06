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
package com.facebook.presto.elasticsearch;

import static java.util.Objects.requireNonNull;

public class Shard
{
    private final int id;
    private final String host;
    private final int port;

    public Shard(int id, String host, int port)
    {
        this.id = id;
        this.host = requireNonNull(host, "host is null");
        this.port = port;
    }

    public int getId()
    {
        return id;
    }

    public String getHost()
    {
        return host;
    }

    public int getPort()
    {
        return port;
    }
}
