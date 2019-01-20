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
package com.facebook.presto.util;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.StringTokenizer;

public abstract class CIDR
        implements Comparable<CIDR>
{
    protected InetAddress baseAddress;
    protected int cidrMask;
    public static CIDR newCIDR(InetAddress baseAddress, int cidrMask)
            throws UnknownHostException
    {
        if (cidrMask < 0) {
            throw new UnknownHostException("Invalid mask length used: " + cidrMask);
        }
        if (baseAddress instanceof Inet4Address) {
            if (cidrMask > 32) {
                throw new UnknownHostException("Invalid mask length used: " + cidrMask);
            }
            return new CIDR4((Inet4Address) baseAddress, cidrMask);
        }
        // IPv6.
        if (cidrMask > 128) {
            throw new UnknownHostException("Invalid mask length used: " + cidrMask);
        }
        return new CIDR6((Inet6Address) baseAddress, cidrMask);
    }

    public static CIDR newCIDR(String cidr)
            throws UnknownHostException
    {
        int p = cidr.indexOf('/');
        if (p < 0) {
            throw new UnknownHostException("Invalid CIDR notation used: " + cidr);
        }
        String addrString = cidr.substring(0, p);
        String maskString = cidr.substring(p + 1);
        InetAddress addr = addressStringToInet(addrString);
        int mask;
        if (maskString.indexOf('.') < 0) {
            mask = parseInt(maskString, -1);
        }
        else {
            mask = getNetMask(maskString);
            if (addr instanceof Inet6Address) {
                mask += 96;
            }
        }
        if (mask < 0) {
            throw new UnknownHostException("Invalid mask length used: " + maskString);
        }
        return newCIDR(addr, mask);
    }

    private static InetAddress addressStringToInet(String addr)
            throws UnknownHostException
    {
        return InetAddress.getByName(addr);
    }

    private static int getNetMask(String netMask)
    {
        StringTokenizer nm = new StringTokenizer(netMask, ".");
        int i = 0;
        int[] netmask = new int[4];
        while (nm.hasMoreTokens()) {
            netmask[i] = Integer.parseInt(nm.nextToken());
            i++;
        }
        int mask1 = 0;
        for (i = 0; i < 4; i++) {
            mask1 += Integer.bitCount(netmask[i]);
        }
        return mask1;
    }

    private static int parseInt(String intstr, int def)
    {
        Integer res;
        if (intstr == null) {
            return def;
        }
        try {
            res = Integer.decode(intstr);
        }
        catch (Exception e) {
            res = def;
        }
        return res.intValue();
    }

    public static byte[] getIpV4FromIpV6(Inet6Address address)
    {
        byte[] baddr = address.getAddress();
        for (int i = 0; i < 9; i++) {
            if (baddr[i] != 0) {
                throw new IllegalArgumentException("This IPv6 address cannot be used in IPv4 context");
            }
        }
        if (baddr[10] != 0 && baddr[10] != 0xFF || baddr[11] != 0 && baddr[11] != 0xFF) {
            throw new IllegalArgumentException("This IPv6 address cannot be used in IPv4 context");
        }
        return new byte[]
                {baddr[12], baddr[13], baddr[14], baddr[15]};
    }

    public static byte[] getIpV6FromIpV4(Inet4Address address)
    {
        byte[] baddr = address.getAddress();
        return new byte[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, baddr[0], baddr[1], baddr[2], baddr[3]};
    }

    public InetAddress getBaseAddress()
    {
        return baseAddress;
    }

    public int getMask()
    {
        return cidrMask;
    }

    @Override
    public String toString()
    {
        return baseAddress.getHostAddress() + '/' + cidrMask;
    }

    public abstract InetAddress getEndAddress();

    public abstract boolean contains(InetAddress inetAddress);

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof CIDR)) {
            return false;
        }
        return compareTo((CIDR) o) == 0;
    }

    @Override
    public int hashCode()
    {
        return baseAddress.hashCode();
    }
}
