===================
IP Functions
===================

.. function:: ip_prefix(ip_address, prefix_bits) -> ipprefix

    Returns the IP prefix of a given ``ip_address`` with subnet size of ``prefix_bits``.
    ``ip_address`` can be either of type ``VARCHAR`` or type ``IPADDRESS``. ::

        SELECT ip_prefix(CAST('192.168.255.255' AS IPADDRESS), 9); -- {192.128.0.0/9}
        SELECT ip_prefix('2001:0db8:85a3:0001:0001:8a2e:0370:7334', 48); -- {2001:db8:85a3::/48}

