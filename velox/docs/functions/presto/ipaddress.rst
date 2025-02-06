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

