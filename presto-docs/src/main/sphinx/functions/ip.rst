===================
IP Functions
===================

.. function:: ip_prefix(ip_address, prefix_bits) -> ipprefix

    Returns the IP prefix of a given ``ip_address`` with subnet size of ``prefix_bits``.
    ``ip_address`` can be either of type ``VARCHAR`` or type ``IPADDRESS``. ::

        SELECT ip_prefix(CAST('192.168.255.255' AS IPADDRESS), 9); -- {192.128.0.0/9}
        SELECT ip_prefix('2001:0db8:85a3:0001:0001:8a2e:0370:7334', 48); -- {2001:db8:85a3::/48}

.. function:: ip_subnet_min(ip_prefix) -> ip_address

    Returns the smallest IP address of type ``IPADDRESS`` in the subnet
    specified by ``ip_prefix``. ::

        SELECT ip_subnet_min(IPPREFIX '192.168.255.255/9'); -- {192.128.0.0}
        SELECT ip_subnet_min(IPPREFIX '2001:0db8:85a3:0001:0001:8a2e:0370:7334/48'); -- {2001:db8:85a3::}

.. function:: ip_subnet_max(ip_prefix) -> ip_address

    Returns the largest IP address of type ``IPADDRESS`` in the subnet
    specified by ``ip_prefix``. ::

        SELECT ip_subnet_max(IPPREFIX '192.64.0.0/9'); -- {192.127.255.255}
        SELECT ip_subnet_max(IPPREFIX '2001:0db8:85a3:0001:0001:8a2e:0370:7334/48'); -- {2001:db8:85a3:ffff:ffff:ffff:ffff:ffff}

.. function:: ip_subnet_range(ip_prefix) -> array(ip_address)

    Return an array of 2 IP addresses.
    The array contains the smallest and the largest IP address
    in the subnet specified by ``ip_prefix``. ::

        SELECT ip_subnet_range(IPPREFIX '1.2.3.160/24'); -- [{1.2.3.0}, {1.2.3.255}]
        SELECT ip_subnet_range(IPPREFIX '64:ff9b::52f4/120'); -- [{64:ff9b::5200}, {64:ff9b::52ff}]

.. function:: is_subnet_of(ip_prefix, ip_address) -> boolean

    Returns ``true`` if the ``ip_address`` is in the subnet of ``ip_prefix``. ::

        SELECT is_subnet_of(IPPREFIX '1.2.3.128/26', IPADDRESS '1.2.3.129'); -- true
        SELECT is_subnet_of(IPPREFIX '64:fa9b::17/64', IPADDRESS '64:ffff::17'); -- false

.. function:: is_subnet_of(ip_prefix1, ip_prefix2) -> boolean

    Returns ``true`` if ``ip_prefix2`` is a subnet of ``ip_prefix1``. ::

        SELECT is_subnet_of(IPPREFIX '192.168.3.131/26', IPPREFIX '192.168.3.144/30'); -- true
        SELECT is_subnet_of(IPPREFIX '64:ff9b::17/64', IPPREFIX '64:ffff::17/64'); -- false
        SELECT is_subnet_of(IPPREFIX '192.168.3.131/26', IPPREFIX '192.168.3.131/26'); -- true

.. function:: ip_prefix_collapse(array(ip_prefix)) -> array(ip_prefix)

    Returns the minimal CIDR representation of the input ``IPPREFIX`` array.
    Every ``IPPREFIX`` in the input array must be the same IP version (that is, only IPv4 or only IPv6)
    or the query will fail and raise an error. ::

        SELECT IP_PREFIX_COLLAPSE(ARRAY[IPPREFIX '192.168.0.0/24', IPPREFIX '192.168.1.0/24']); -- [{192.168.0.0/23}]
        SELECT IP_PREFIX_COLLAPSE(ARRAY[IPPREFIX '2620:10d:c090::/48', IPPREFIX '2620:10d:c091::/48']); -- [{2620:10d:c090::/47}]
        SELECT IP_PREFIX_COLLAPSE(ARRAY[IPPREFIX '192.168.1.0/24', IPPREFIX '192.168.0.0/24', IPPREFIX '192.168.2.0/24', IPPREFIX '192.168.9.0/24']); -- [{192.168.0.0/23}, {192.168.2.0/24}, {192.168.9.0/24}]

.. function:: is_private_ip(ip_address) -> boolean

    Returns whether ``ip_address`` of type ``IPADDRESS`` is a private or reserved IP address
    that is not considered globally reachable by IANA. For more information, see `IANA IPv4 Special-Purpose Address Registry <https://www.iana.org/assignments/iana-ipv4-special-registry/iana-ipv4-special-registry.xhtml>`_ and `IANA IPv6 Special-Purpose Address Registry <https://www.iana.org/assignments/iana-ipv6-special-registry/iana-ipv6-special-registry.xhtml>`_. `Null` inputs return `null`. ::

        SELECT is_private_ip(IPADDRESS '10.0.0.1'); -- true
        SELECT is_private_ip(IPADDRESS '192.168.0.1'); -- true
        SELECT is_private_ip(IPADDRESS '157.240.200.99'); -- false
        SELECT is_private_ip(IPADDRESS '2a03:2880:f031:12:face:b00c:0:2'); -- false

.. function:: ip_prefix_subnets(ip_prefix, prefix_length) -> array(ip_prefix)

    Returns the subnets of ``ip_prefix`` of size ``prefix_length``. ``prefix_length`` must be valid ([0, 32] for IPv4
    and [0, 128] for IPv6) or the query will fail and raise an error. An empty array is returned if ``prefix_length``
    is shorter (that is, less specific) than ``ip_prefix``. ::

        SELECT IP_PREFIX_SUBNETS(IPPREFIX '192.168.1.0/24', 25); -- [{192.168.1.0/25}, {192.168.1.128/25}]
        SELECT IP_PREFIX_SUBNETS(IPPREFIX '2a03:2880:c000::/34', 36); -- [{2a03:2880:c000::/36}, {2a03:2880:d000::/36}, {2a03:2880:e000::/36}, {2a03:2880:f000::/36}]
