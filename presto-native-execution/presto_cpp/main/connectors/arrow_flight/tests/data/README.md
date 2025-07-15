### Placeholder TLS & mTLS Certificates for Arrow Flight Connector Unit Testing
The `tls_certs` and `mtls_certs` directories contains placeholder certificates used for unit testing the Arrow Flight Connector with TLS and mutual TLS (mTLS) enabled. These certificates are **not intended for production use** and should only be used in the context of unit or integration tests.

### Generating TLS Certificates
To create the TLS certificates and keys inside the `tls_certs` folder, run the following command:

`./generate_tls_certs.sh`

### Generating mTLS Certificates
To create a full set of mTLS certificates (CA, server, and client) for mutual TLS testing inside the `mtls_certs` folder, run:

`./generate_mtls_certs.sh`
