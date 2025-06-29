/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.hive;

import com.facebook.airlift.log.Logger;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.Security;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Date;

public class HiveSslTestKeystoreManager
{
    private static final Logger log = Logger.get(HiveSslTestKeystoreManager.class);
    private static boolean initialized;
    private static String jksFilesPath;
    private static File keyStoreFile;
    private static File trustStoreFile;

    private HiveSslTestKeystoreManager()
    {
    }

    public static synchronized void initializeKeystore()
    {
        try {
            if (initialized) {
                return;
            }

            jksFilesPath = Paths.get((HiveSslTestKeystoreManager.class.getResource("/hive_ssl_enable")).toURI()).toFile().toString();
            keyStoreFile = Paths.get(jksFilesPath, "hive-metastore.jks").toFile();
            trustStoreFile = Paths.get(jksFilesPath, "hive-metastore-truststore.jks").toFile();

            if (keyStoreFile.exists() && trustStoreFile.exists()) {
                initialized = true;
                return;
            }
            generateKeyStoreFiles();
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to generate keystore files at path: " + jksFilesPath, e);
        }
    }

    static void generateKeyStoreFiles() throws Exception
    {
        Security.addProvider(new BouncyCastleProvider());

        String alias = "hive-metastore";
        char[] password = "123456".toCharArray();
        String certFile = "server.cer";
        int validityDays = 100000;

        // 1. Generate RSA KeyPair
        KeyPair keyPair = generateRSAKeyPair();

        // 2. Generate self-signed certificate
        X509Certificate cert = generateSelfSignedCertificate(keyPair, alias, validityDays);

        // 3. Create Keystore and save key + cert
        KeyStore keyStore = KeyStore.getInstance("JKS");
        keyStore.load(null, null);
        keyStore.setKeyEntry(alias, keyPair.getPrivate(), password, new Certificate[]{cert});

        try (FileOutputStream fos = new FileOutputStream(keyStoreFile)) {
            keyStore.store(fos, password);
        }

        File certFilePath = Paths.get(jksFilesPath, certFile).toFile();

        // 4. Export certificate to file (DER encoded)
        try (FileOutputStream fos = new FileOutputStream(certFilePath)) {
            fos.write(cert.getEncoded());
        }
        log.info("Certificate exported to: " + certFilePath);

        // 5. Create truststore and import certificate
        KeyStore trustStore = KeyStore.getInstance("JKS");
        trustStore.load(null, null);
        trustStore.setCertificateEntry(alias, cert);

        try (FileOutputStream fos = new FileOutputStream(trustStoreFile)) {
            trustStore.store(fos, password);
        }
    }

    private static KeyPair generateRSAKeyPair() throws NoSuchAlgorithmException
    {
        KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
        keyGen.initialize(2048);
        return keyGen.generateKeyPair();
    }

    private static X509Certificate generateSelfSignedCertificate(KeyPair keyPair, String dn, int validityDays)
            throws OperatorCreationException, CertificateException, IOException
    {
        long now = System.currentTimeMillis();
        Date startDate = new Date(now);

        X500Name issuer = new X500Name("CN=" + dn + ", OU=, O=, L=, ST=, C=");
        BigInteger serialNumber = new BigInteger(64, new SecureRandom());
        Date endDate = new Date(now + validityDays * 24L * 60L * 60L * 1000L);

        // Use SHA256withRSA
        ContentSigner contentSigner = new JcaContentSignerBuilder("SHA256withRSA")
                .build(keyPair.getPrivate());

        JcaX509v3CertificateBuilder certBuilder = new JcaX509v3CertificateBuilder(
                issuer,
                serialNumber,
                startDate,
                endDate,
                issuer,
                keyPair.getPublic());

        X509CertificateHolder certHolder = certBuilder.build(contentSigner);
        return new JcaX509CertificateConverter()
                .setProvider("BC")
                .getCertificate(certHolder);
    }

    public static String getKeystorePath()
    {
        initializeKeystore();
        if (keyStoreFile == null || !keyStoreFile.exists()) {
            throw new IllegalStateException("Keystore file is not initialized or missing");
        }
        return keyStoreFile.getAbsolutePath();
    }

    public static String getTruststorePath()
    {
        initializeKeystore();
        if (trustStoreFile == null || !trustStoreFile.exists()) {
            throw new IllegalStateException("Keystore file is not initialized or missing");
        }
        return trustStoreFile.getAbsolutePath();
    }
}
