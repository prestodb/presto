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
package io.prestosql.plugin.mongodb;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.backend.memory.MemoryBackend;
import de.bwaldvogel.mongo.backend.memory.MemoryDatabase;
import de.bwaldvogel.mongo.exception.MongoServerException;
import io.netty.channel.Channel;
import org.bson.BSONObject;

public class SyncMemoryBackend
        extends MemoryBackend
{
    @Override
    public MemoryDatabase openOrCreateDatabase(String databaseName)
            throws MongoServerException
    {
        return new SyncMemoryDatabase(this, databaseName);
    }

    private static class SyncMemoryDatabase
            extends MemoryDatabase
    {
        public SyncMemoryDatabase(MongoBackend backend, String databaseName)
                throws MongoServerException
        {
            super(backend, databaseName);
        }

        @Override
        public synchronized BSONObject handleCommand(Channel channel, String command, BSONObject query)
                throws MongoServerException
        {
            return super.handleCommand(channel, command, query);
        }
    }
}
