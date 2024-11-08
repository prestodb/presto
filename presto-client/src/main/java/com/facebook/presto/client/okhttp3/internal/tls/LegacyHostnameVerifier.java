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
package com.facebook.presto.client.okhttp3.internal.tls;

import okhttp3.internal.tls.OkHostnameVerifier;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;
import javax.security.auth.x500.X500Principal;

import java.security.cert.Certificate;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

/**
 * The type Legacy hostname verifier.
 */
public class LegacyHostnameVerifier
        implements HostnameVerifier
{
    private static final int ALT_DNS_NAME = 2;
    private static final int ALT_IPA_NAME = 7;
    private static final Pattern VERIFY_AS_IP_ADDRESS = Pattern.compile(
            "([0-9a-fA-F]*:[0-9a-fA-F:.]*)|([\\d.]+)");

    /**
     * The constant INSTANCE.
     */
    public static final HostnameVerifier INSTANCE = new LegacyHostnameVerifier();

    private LegacyHostnameVerifier()
    {
    }

    @Override
    public boolean verify(String host, SSLSession session)
    {
        if (OkHostnameVerifier.INSTANCE.verify(host, session)) {
            return true;
        }

        if (verifyAsIpAddress(host)) {
            return false;
        }

        try {
            Certificate[] certificates = session.getPeerCertificates();
            X509Certificate certificate = (X509Certificate) certificates[0];

            if (!allSubjectAltNames(certificate).isEmpty()) {
                return false;
            }

            X500Principal principal = certificate.getSubjectX500Principal();
            String cn = new DistinguishedNameParser(principal).findMostSpecific("cn");
            if (cn != null) {
                return verifyHostName(host, cn);
            }

            return false;
        }
        catch (SSLException e) {
            return false;
        }
    }

    /**
     * Verify as ip address boolean.
     *
     * @param host the host
     * @return the boolean
     */
    static boolean verifyAsIpAddress(String host)
    {
        return VERIFY_AS_IP_ADDRESS.matcher(host).matches();
    }

    /**
     * All subject alt names list.
     *
     * @param certificate the certificate
     * @return the list
     */
    public static List<String> allSubjectAltNames(X509Certificate certificate)
    {
        List<String> altIpaNames = getSubjectAltNames(certificate, ALT_IPA_NAME);
        List<String> altDnsNames = getSubjectAltNames(certificate, ALT_DNS_NAME);
        List<String> result = new ArrayList<>(altIpaNames.size() + altDnsNames.size());
        result.addAll(altIpaNames);
        result.addAll(altDnsNames);
        return result;
    }

    private static List<String> getSubjectAltNames(X509Certificate certificate, int type)
    {
        List<String> result = new ArrayList<>();
        try {
            Collection<?> subjectAltNames = certificate.getSubjectAlternativeNames();
            if (subjectAltNames == null) {
                return Collections.emptyList();
            }
            for (Object subjectAltName : subjectAltNames) {
                List<?> entry = (List<?>) subjectAltName;
                if (entry == null || entry.size() < 2) {
                    continue;
                }
                Integer altNameType = (Integer) entry.get(0);
                if (altNameType == null) {
                    continue;
                }
                if (altNameType == type) {
                    String altName = (String) entry.get(1);
                    if (altName != null) {
                        result.add(altName);
                    }
                }
            }
            return result;
        }
        catch (CertificateParsingException e) {
            return Collections.emptyList();
        }
    }

    /**
     * Returns {@code true} iff {@code hostName} matches the domain name {@code pattern}.
     *
     * @param hostName lower-case host name.
     * @param pattern  domain name pattern from certificate. May be a wildcard pattern such as
     *                 {@code *.android.com}.
     */
    private boolean verifyHostName(String hostName, String pattern)
    {
        if ((hostName == null) || (hostName.length() == 0) || (hostName.startsWith("."))
                || (hostName.endsWith(".."))) {
            return false;
        }
        if ((pattern == null) || (pattern.length() == 0) || (pattern.startsWith("."))
                || (pattern.endsWith(".."))) {
            return false;
        }

        if (!hostName.endsWith(".")) {
            hostName += '.';
        }
        if (!pattern.endsWith(".")) {
            pattern += '.';
        }

        pattern = pattern.toLowerCase(Locale.US);

        if (!pattern.contains("*")) {
            return hostName.equals(pattern);
        }

        if ((!pattern.startsWith("*.")) || (pattern.indexOf('*', 1) != -1)) {
            return false;
        }

        if (hostName.length() < pattern.length()) {
            return false;
        }

        if ("*.".equals(pattern)) {
            return false;
        }

        String suffix = pattern.substring(1);
        if (!hostName.endsWith(suffix)) {
            return false;
        }

        int suffixStartIndexInHostName = hostName.length() - suffix.length();
        if ((suffixStartIndexInHostName > 0)
                && (hostName.lastIndexOf('.', suffixStartIndexInHostName - 1) != -1)) {
            return false;
        }
        return true;
    }
}
