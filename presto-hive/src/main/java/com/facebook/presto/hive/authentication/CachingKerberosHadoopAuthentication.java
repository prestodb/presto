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
package com.facebook.presto.hive.authentication;

import io.airlift.units.Duration;
import org.apache.hadoop.security.UserGroupInformation;

import javax.annotation.concurrent.GuardedBy;
import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosTicket;

import static com.facebook.presto.hive.authentication.KerberosTicketUtils.getTicketGrantingTicket;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Long.min;
import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.security.UserGroupInformationShim.getSubject;

public class CachingKerberosHadoopAuthentication
        implements HadoopAuthentication
{
    private final KerberosHadoopAuthentication delegate;
    private final long maxCacheDurationMillis;

    private final Object lock = new Object();
    @GuardedBy("lock")
    private UserGroupInformation userGroupInformation = null;
    @GuardedBy("lock")
    private long nextRefreshTime = Long.MIN_VALUE;

    public CachingKerberosHadoopAuthentication(KerberosHadoopAuthentication delegate, Duration maxCacheDuration)
    {
        this.delegate = requireNonNull(delegate, "hadoopAuthentication is null");
        this.maxCacheDurationMillis = requireNonNull(maxCacheDuration, "maxCacheDuration is null").toMillis();
    }

    @Override
    public UserGroupInformation getUserGroupInformation()
    {
        synchronized (lock) {
            if (refreshIsNeeded()) {
                refreshUgi();
            }
        }
        return userGroupInformation;
    }

    private void refreshUgi()
    {
        userGroupInformation = delegate.getUserGroupInformation();
        nextRefreshTime = min(calculateNextRefreshTime(userGroupInformation), System.currentTimeMillis() + maxCacheDurationMillis);
    }

    private boolean refreshIsNeeded()
    {
        return nextRefreshTime < System.currentTimeMillis() || userGroupInformation == null;
    }

    private long calculateNextRefreshTime(UserGroupInformation userGroupInformation)
    {
        Subject subject = getSubject(userGroupInformation);
        checkArgument(subject != null, "subject must be present in kerberos based UGI");
        KerberosTicket tgtTicket = getTicketGrantingTicket(subject);
        return KerberosTicketUtils.getRefreshTime(tgtTicket);
    }
}
