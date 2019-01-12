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
package io.prestosql.plugin.hive.authentication;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KerberosTicket;

import java.util.Set;

final class KerberosTicketUtils
{
    private static final float TICKET_RENEW_WINDOW = 0.80f;

    private KerberosTicketUtils()
    {
    }

    static KerberosTicket getTicketGrantingTicket(Subject subject)
    {
        Set<KerberosTicket> tickets = subject.getPrivateCredentials(KerberosTicket.class);
        for (KerberosTicket ticket : tickets) {
            if (isOriginalTicketGrantingTicket(ticket)) {
                return ticket;
            }
        }
        throw new IllegalArgumentException("kerberos ticket not found in " + subject);
    }

    static long getRefreshTime(KerberosTicket ticket)
    {
        long start = ticket.getStartTime().getTime();
        long end = ticket.getEndTime().getTime();
        return start + (long) ((end - start) * TICKET_RENEW_WINDOW);
    }

    /**
     * Check whether the server principal is the TGS's principal
     *
     * @param ticket the original TGT (the ticket that is obtained when a
     * kinit is done)
     * @return true or false
     */
    static boolean isOriginalTicketGrantingTicket(KerberosTicket ticket)
    {
        return isTicketGrantingServerPrincipal(ticket.getServer());
    }

    /**
     * TGS must have the server principal of the form "krbtgt/FOO@FOO".
     *
     * @return true or false
     */
    private static boolean isTicketGrantingServerPrincipal(KerberosPrincipal principal)
    {
        if (principal == null) {
            return false;
        }
        if (principal.getName().equals("krbtgt/" + principal.getRealm() + "@" + principal.getRealm())) {
            return true;
        }
        return false;
    }
}
