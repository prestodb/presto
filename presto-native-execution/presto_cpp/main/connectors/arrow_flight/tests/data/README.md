### Dummy TLS Certificates for Arrow Flight Connector Unit Testing
The `tls_certs` directory contains dummy TLS certificates generated specifically for unit testing the Arrow Flight Connector with TLS enabled. These certificates are not intended for production use and should only be used in the context of unit tests.

### Generating TLS Certificates
The certificates are generated using the following command:
`./generate_tls_certs.sh`
This will create the necessary certificates and keys inside the `tls_certs` folder.