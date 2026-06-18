### Placeholder TLS & mTLS Certificates for Arrow Flight Connector Unit Testing
The `certs/` directory contains placeholder certificates used for unit and integration testing of the Arrow Flight Connector with **TLS** and **mutual TLS (mTLS)** enabled. These certificates are **not intended for production use** and should only be used for local development or automated testing scenarios.

### Generating Certificates for TLS and mTLS Testing
To generate the necessary certificates and keys for both TLS and mTLS testing, run the following script:

`./generate_certs.sh`
