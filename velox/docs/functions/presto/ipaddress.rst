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
