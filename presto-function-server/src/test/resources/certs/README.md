# Test Certificate Fixtures

This directory contains the TLS certificate fixtures used by the
`presto-function-server` mTLS + JWT integration tests.

All long-lived certificates use 100-year validity (`-days 36500`) and EC prime256v1 keys.
Run all commands from the root of the `certs/` directory.

## Certificate inventory and expiry

| File | Role | CN / Owner | Expires |
|---|---|---|---|
| `ca/ca.crt` | Root CA — signs all leaf certs | Presto Test Root CA | **2036-05-26** |
| `truststore.jks` | Shared truststore containing `ca.crt` (password: `changeit`) | — | (CA expiry) |
| `coordinator/coordinator-keystore.jks` | Coordinator TLS identity (password: `changeit`) | localhost / Coordinator | **2126-07-28** |
| `function-server/function-server-keystore.jks` | Function Server TLS identity (password: `changeit`) | localhost / FunctionServer | **2126-07-28** |
| `function-server/invalid-keystore.jks` | Cert signed by a **different, untrusted CA** — used for negative rejection tests | localhost | **2126-07-28** |
| `function-server/expired-keystore.jks` | **Already-expired** leaf cert (valid 2020-01-01 → 2020-01-02) — client presents this in `testExpiredCertificateIsRejected` | localhost | **2020-01-02** |
| `expired-truststore.jks` | Truststore containing the expired CA; loaded by the function server so the issuer is recognised and the *only* rejection reason is cert expiry (password: `changeit`) | — | **2020-01-02** |
| `worker/worker-keystore.jks` | Worker TLS identity (password: `changeit`) | localhost / Worker | **2126-07-28** |
| `worker/worker.crt` | Worker cert in PEM form (same key pair as `worker-keystore.jks`) | localhost / Worker | **2126-07-29** |
| `worker/worker.key` | Worker private key in PEM form | — | — |
| `worker/worker-combined.pem` | `worker.crt` + `worker.key` concatenated — used by the native C++ worker | — | **2126-07-29** |

> **Leaf certs expire 2126. The CA expires 2036-05-26.**
> When the CA expires, regenerate it and re-sign all leaf certs using the
> commands below. Leaf certs do not need to be touched before 2126.
>
> **Expired fixtures** (`function-server/expired-keystore.jks`, `expired-truststore.jks`)
> are intentionally short-lived and do not need to be regenerated — they must stay expired.

---

## CA Certificate

```bash
# Generate CA private key
openssl ecparam -name prime256v1 -genkey -noout -out ca/ca.key
```

```bash
# Generate self-signed CA certificate
openssl req -x509 \
  -new \
  -key ca/ca.key \
  -out ca/ca.crt \
  -days 36500 \
  -subj "/CN=Presto CA"
```

---

## Coordinator Certificate

Create `coordinator/coordinator.cnf`:

```ini
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req
prompt = no

[req_distinguished_name]
C = US
ST = California
L = San Francisco
O = Presto
OU = Coordinator
CN = localhost

[v3_req]
basicConstraints = CA:FALSE
keyUsage = critical,digitalSignature,keyEncipherment
extendedKeyUsage = serverAuth,clientAuth
subjectAltName = @alt_names

[alt_names]
DNS.1 = localhost
DNS.2 = coordinator
DNS.3 = *.local
DNS.4 = presto-coordinator
IP.1 = 127.0.0.1
IP.2 = 0.0.0.0
IP.3 = ::1
IP.4 = ::
```

```bash
# Generate coordinator private key
openssl ecparam -name prime256v1 -genkey -noout -out coordinator/coordinator.key
```

```bash
# Generate CSR
openssl req -new \
  -key coordinator/coordinator.key \
  -out coordinator/coordinator.csr \
  -config coordinator/coordinator.cnf
```

```bash
# Sign with CA (100-year validity)
openssl x509 -req \
  -in coordinator/coordinator.csr \
  -CA ca/ca.crt \
  -CAkey ca/ca.key \
  -CAcreateserial \
  -out coordinator/coordinator.crt \
  -days 36500 \
  -extensions v3_req \
  -extfile coordinator/coordinator.cnf
```

```bash
# Export to PKCS12
openssl pkcs12 -export \
  -in coordinator/coordinator.crt \
  -inkey coordinator/coordinator.key \
  -out coordinator/coordinator.p12 \
  -name coordinator \
  -passout pass:changeit
```

```bash
# Convert to JKS
keytool -importkeystore \
  -srckeystore coordinator/coordinator.p12 \
  -srcstoretype PKCS12 \
  -srcstorepass changeit \
  -destkeystore coordinator/coordinator-keystore.jks \
  -deststoretype JKS \
  -deststorepass changeit \
  -noprompt
```

---

## Function Server Certificate

Create `function-server/function-server.cnf`:

```ini
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req
prompt = no

[req_distinguished_name]
C = US
ST = California
L = San Francisco
O = Presto
OU = FunctionServer
CN = localhost

[v3_req]
basicConstraints = CA:FALSE
keyUsage = critical,digitalSignature,keyEncipherment
extendedKeyUsage = serverAuth,clientAuth
subjectAltName = @alt_names

[alt_names]
DNS.1 = localhost
DNS.2 = function-server
DNS.3 = presto-remote-function-server
IP.1 = 127.0.0.1
```

```bash
# Generate function server private key
openssl ecparam -name prime256v1 -genkey -noout -out function-server/function-server.key
```

```bash
# Generate CSR
openssl req -new \
  -key function-server/function-server.key \
  -out function-server/function-server.csr \
  -config function-server/function-server.cnf
```

```bash
# Sign with CA (100-year validity)
openssl x509 -req \
  -in function-server/function-server.csr \
  -CA ca/ca.crt \
  -CAkey ca/ca.key \
  -CAcreateserial \
  -out function-server/function-server.crt \
  -days 36500 \
  -extensions v3_req \
  -extfile function-server/function-server.cnf
```

```bash
# Export to PKCS12
openssl pkcs12 -export \
  -in function-server/function-server.crt \
  -inkey function-server/function-server.key \
  -out function-server/function-server.p12 \
  -name function-server \
  -passout pass:changeit
```

```bash
# Convert to JKS
keytool -importkeystore \
  -srckeystore function-server/function-server.p12 \
  -srcstoretype PKCS12 \
  -srcstorepass changeit \
  -destkeystore function-server/function-server-keystore.jks \
  -deststoretype JKS \
  -deststorepass changeit \
  -noprompt
```

---

## Worker Certificate

Create `worker/worker.cnf`:

```ini
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req
prompt = no

[req_distinguished_name]
C = US
ST = California
L = San Francisco
O = Presto
OU = Worker
CN = localhost

[v3_req]
basicConstraints = CA:FALSE
keyUsage = critical,digitalSignature,keyEncipherment
extendedKeyUsage = serverAuth,clientAuth
subjectAltName = @alt_names

[alt_names]
DNS.1 = localhost
DNS.2 = worker
DNS.3 = *.local
DNS.4 = native-worker-0
DNS.5 = native-worker-1
DNS.6 = native-worker-2
DNS.7 = native-worker-3
IP.1 = 127.0.0.1
IP.2 = 0.0.0.0
IP.3 = ::1
IP.4 = ::
```

```bash
# Generate worker private key
openssl ecparam -name prime256v1 -genkey -noout -out worker/worker.key
```

```bash
# Generate CSR
openssl req -new \
  -key worker/worker.key \
  -out worker/worker.csr \
  -config worker/worker.cnf
```

```bash
# Sign with CA (100-year validity)
openssl x509 -req \
  -in worker/worker.csr \
  -CA ca/ca.crt \
  -CAkey ca/ca.key \
  -CAcreateserial \
  -out worker/worker.crt \
  -days 36500 \
  -extensions v3_req \
  -extfile worker/worker.cnf
```

```bash
# Generate combined PEM (cert + key concatenated — used by native C++ worker)
cat worker/worker.crt worker/worker.key > worker/worker-combined.pem
```

```bash
# Export to PKCS12
openssl pkcs12 -export \
  -in worker/worker.crt \
  -inkey worker/worker.key \
  -out worker/worker.p12 \
  -name worker \
  -passout pass:changeit
```

```bash
# Convert to JKS
keytool -importkeystore \
  -srckeystore worker/worker.p12 \
  -srcstoretype PKCS12 \
  -srcstorepass changeit \
  -destkeystore worker/worker-keystore.jks \
  -deststoretype JKS \
  -deststorepass changeit \
  -noprompt
```

---

## Invalid Keystore (for negative tests)

Signed by a throwaway CA that is **not** in `truststore.jks` — used to verify
the Function Server rejects clients presenting an untrusted certificate.

```bash
# Generate throwaway CA
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 \
  -keyout invalid/invalid-ca.key \
  -out invalid/invalid-ca.crt \
  -days 36500 -nodes \
  -subj "/CN=InvalidCA"
```

```bash
# Generate invalid private key
openssl ecparam -name prime256v1 -genkey -noout -out invalid/invalid.key
```

```bash
# Generate CSR
openssl req -new \
  -key invalid/invalid.key \
  -out invalid/invalid.csr \
  -subj "/CN=localhost"
```

```bash
# Sign with the untrusted CA
openssl x509 -req \
  -in invalid/invalid.csr \
  -CA invalid/invalid-ca.crt \
  -CAkey invalid/invalid-ca.key \
  -CAcreateserial \
  -out invalid/invalid.crt \
  -days 36500
```

```bash
# Export to PKCS12
openssl pkcs12 -export \
  -in invalid/invalid.crt \
  -inkey invalid/invalid.key \
  -out invalid/invalid.p12 \
  -name invalid \
  -passout pass:changeit
```

```bash
# Convert to JKS
keytool -importkeystore \
  -srckeystore invalid/invalid.p12 \
  -srcstoretype PKCS12 \
  -srcstorepass changeit \
  -destkeystore function-server/invalid-keystore.jks \
  -deststoretype JKS \
  -deststorepass changeit \
  -noprompt
```

---

## Truststore

```bash
# Build truststore.jks from the CA certificate
keytool -importcert \
  -alias ca \
  -file ca/ca.crt \
  -keystore truststore.jks \
  -storetype JKS \
  -storepass changeit \
  -noprompt
```

---

## Expired Keystore and Truststore (for negative tests)

The expired CA and leaf cert are valid from **2020-01-01 to 2020-01-02** — already
expired.  They are committed as binary fixtures and **must not be regenerated** (they
need to remain expired forever).

The commands below are provided only as documentation of how they were originally
created.  Run them from the root of the `certs/` directory if you ever need to
recreate them from scratch (OpenSSL 3+ required for `-not_before` / `-not_after`).

```bash
# 1. Generate expired CA (validity window entirely in the past)
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 \
  -keyout expired/expired-ca.key \
  -out expired/expired-ca.crt \
  -noenc \
  -subj "/CN=Presto Expired Test CA" \
  -not_before 20200101000000Z \
  -not_after  20200102000000Z
```

```bash
# 2. Generate expired leaf private key
openssl ecparam -name prime256v1 -genkey -noout -out expired/expired.key
```

```bash
# 3. Generate CSR
openssl req -new \
  -key expired/expired.key \
  -out expired/expired.csr \
  -subj "/CN=localhost"
```

```bash
# 4. Sign leaf with the expired CA (also in the past window)
openssl x509 -req \
  -in expired/expired.csr \
  -CA expired/expired-ca.crt \
  -CAkey expired/expired-ca.key \
  -CAcreateserial \
  -out expired/expired.crt \
  -not_before 20200101000000Z \
  -not_after  20200102000000Z
```

```bash
# 5. Export leaf to PKCS12
openssl pkcs12 -export \
  -in expired/expired.crt \
  -inkey expired/expired.key \
  -out expired/expired.p12 \
  -name expired \
  -passout pass:changeit
```

```bash
# 6. Convert to JKS — this is the keystore the coordinator presents as its identity
keytool -importkeystore \
  -srckeystore expired/expired.p12 \
  -srcstoretype PKCS12 \
  -srcstorepass changeit \
  -destkeystore function-server/expired-keystore.jks \
  -deststoretype JKS \
  -deststorepass changeit \
  -noprompt
```

```bash
# 7. Build expired-truststore.jks — loaded by the function server so that the issuer
#    of the expired cert IS trusted; this isolates the failure to cert expiry only.
keytool -importcert \
  -alias expired-ca \
  -file expired/expired-ca.crt \
  -keystore expired-truststore.jks \
  -storetype JKS \
  -storepass changeit \
  -noprompt
```
