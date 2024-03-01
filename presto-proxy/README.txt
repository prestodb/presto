# Proxy service for Presto

This proxy server allows clients to securely access a remote Presto
server (or set of servers) without having direct access to the server.
For example, if the Presto server is behind a firewall, only the proxy
needs to be exposed to the client.

The proxy natively understands the Presto protocol and rewrites the
"nextUri" in response payloads to point to the proxy rather than the
remote server.

If so configured, the proxy will generate JWT access tokens containing
the principal from the TLS client certificate presented to the proxy.
This allows using the proxy in secure environments without needing
the proxy to perform explicit validation of the username and principal.

Note that the proxy does not attempt to hide the remote URIs from
clients. Hostnames and IP addresses will be visible to the client.
