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
package com.facebook.presto.sql.planner;

import com.google.common.base.Charsets;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

import javax.annotation.concurrent.GuardedBy;

import static com.google.common.base.Preconditions.checkState;

public class PlanDigest
{
    private final Hasher hasher;
    private HashCode hashCode;

    @GuardedBy("this")
    private boolean done;

    public PlanDigest()
    {
        HashFunction hashFunction = Hashing.sha256();
        this.hasher = hashFunction.newHasher();
    }

    public synchronized void update(byte[] bytes)
    {
        checkNotDone();
        hasher.putBytes(bytes);
    }

    public synchronized void update(String string)
    {
        checkNotDone();
        hasher.putString(string, Charsets.UTF_8);
        hasher.putChar('|');
    }

    public synchronized void update(PlanDigest planDigest)
    {
        checkNotDone();
        update(planDigest.digest());
    }

    private synchronized void checkNotDone()
    {
        checkState(!done, "Attempt to update a finalized digest.");
    }

    /**
     * Returns the digest as hexadecimal string.
     */
    public synchronized String getDigestString()
    {
        if (!done) {
            return hash().toString();
        }
        return hashCode.toString();
    }

    public byte[] digest()
    {
        return hash().asBytes();
    }

    /**
     * Completes the hash computation on this digest object. Further update attempts will
     * not be possible.
     */
    private synchronized HashCode hash()
    {
        if (!done) {
            done = true;
            hashCode = hasher.hash();
        }
        return hashCode;
    }
}
