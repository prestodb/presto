====================
IP Address Functions
====================


.. function:: contains(format, address) -> boolean
    :noindex:

    Returns true if the ``address`` exists in the CIDR ``format``::

        SELECT contains('10.0.0.1/8', IPADDRESS '10.255.255.255'); -- true
        SELECT contains('10.0.0.1/8', IPADDRESS '11.255.255.255'); -- false

