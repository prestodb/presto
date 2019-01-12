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
package com.facebook.presto.jdbc;

import com.facebook.presto.spi.PrestoWarning;
import com.facebook.presto.spi.WarningCode;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.sql.SQLWarning;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;

@ThreadSafe
public class WarningsManager
{
    @GuardedBy("this")
    private Set<WarningCode> warningsSeen = new HashSet<>();

    @GuardedBy("this")
    private SQLWarning firstWarning;

    // To avoid O(n) behavior of SQLException.setNextException keep a reference to the last warning added:
    @GuardedBy("this")
    private SQLWarning lastWarning;

    private synchronized void addWarning(PrestoWarning warning)
    {
        requireNonNull(warning, "warning is null");
        if (lastWarning == null) {
            lastWarning = new PrestoSqlWarning(warning);
        }
        else {
            lastWarning.setNextWarning(new PrestoSqlWarning(warning));
        }
        if (firstWarning == null) {
            firstWarning = lastWarning;
        }
        else {
            lastWarning = lastWarning.getNextWarning();
        }
    }

    public synchronized void addWarnings(List<PrestoWarning> warnings)
    {
        for (PrestoWarning warning : warnings) {
            if (warningsSeen.add(warning.getWarningCode())) {
                addWarning(warning);
            }
        }
    }

    public synchronized SQLWarning getWarnings()
    {
        return firstWarning;
    }

    public synchronized void clearWarnings()
    {
        firstWarning = null;
        lastWarning = null;
    }
}
