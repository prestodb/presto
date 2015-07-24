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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Random object cache
 */
public class RandCache
{
    private RandCache() {}

    public static class Session
    {
        private final long startTime;
        private final long seed;

        public Session(long startTime, long seed)
        {
            this.startTime = startTime;
            this.seed = seed;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (obj instanceof Session) {
                Session other = (Session) obj;
                return this.startTime == other.startTime && this.seed == other.seed;
            }
            return false;
        }

        @Override
        public int hashCode()
        {
            return (int) (startTime * 31 + seed);
        }
    }

    private static LoadingCache<Session, Random> randCache =
            CacheBuilder
                    .newBuilder()
                    .expireAfterAccess(10, TimeUnit.MINUTES)
                    .build(new CacheLoader<Session, Random>()
                    {
                        @Override
                        public Random load(Session session)
                                throws Exception
                        {
                            return new Random(session.seed);
                        }
                    });

    public static Random get(Session session)
    {
        try {
            return randCache.get(session);
        }
        catch (ExecutionException e) {
            throw new PrestoException(StandardErrorCode.INTERNAL_ERROR, e);
        }
    }
}
