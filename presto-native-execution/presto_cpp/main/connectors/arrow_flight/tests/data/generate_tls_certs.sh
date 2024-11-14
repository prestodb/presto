#!/bin/bash

# Set directory for certificates and keys.
CERT_DIR="./tls_certs"
mkdir -p $CERT_DIR

# Dummy values for the certificates.
COUNTRY="US"
STATE="State"
LOCALITY="City"
ORGANIZATION="MyOrg"
ORG_UNIT="MyUnit"
COMMON_NAME="MyCA"
SERVER_CN="server.mydomain.com"

# Step 1: Generate CA private key and self-signed certificate.
openssl genpkey -algorithm RSA -out $CERT_DIR/ca.key
openssl req -key $CERT_DIR/ca.key -new -x509 -out $CERT_DIR/ca.crt -days 365000 \
    -subj "/C=$COUNTRY/ST=$STATE/L=$LOCALITY/O=$ORGANIZATION/OU=$ORG_UNIT/CN=$COMMON_NAME"

# Step 2: Generate server private key.
openssl genpkey -algorithm RSA -out $CERT_DIR/server.key

# Step 3: Generate server certificate signing request (CSR).
openssl req -new -key $CERT_DIR/server.key -out $CERT_DIR/server.csr \
    -subj "/C=$COUNTRY/ST=$STATE/L=$LOCALITY/O=$ORGANIZATION/OU=$ORG_UNIT/CN=$SERVER_CN" \
    -addext "subjectAltName=DNS:$COMMON_NAME,DNS:localhost" \

# Step 4: Sign server CSR with the CA certificate to generate the server certificate.
openssl x509 -req -in $CERT_DIR/server.csr -CA $CERT_DIR/ca.crt -CAkey $CERT_DIR/ca.key \
    -CAcreateserial -out $CERT_DIR/server.crt -days 365000 \
    -extfile <(printf "subjectAltName=DNS:$COMMON_NAME,DNS:localhost")

# Step 5: Output the generated files.
echo "Certificate Authority (CA) certificate: $CERT_DIR/ca.crt"
echo "Server certificate: $CERT_DIR/server.crt"
echo "Server private key: $CERT_DIR/server.key"

# Step 6: Remove unused files.
rm -rf $CERT_DIR/server.csr $CERT_DIR/ca.srl $CERT_DIR/ca.key
