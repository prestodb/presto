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
package com.facebook.presto.plugin.base.security;

import com.facebook.airlift.log.Logger;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

import java.io.File;
import java.io.FileOutputStream;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.Security;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Date;

public class MongoSslTestCertificateManager
{
    private static final Logger log = Logger.get(MongoSslTestCertificateManager.class);
    private static final String PASSWORD = "password";
    private static final int VALIDITY_DAYS = 100000;

    private MongoSslTestCertificateManager()
    {
        // Utility class - prevent instantiation
    }

    public static TestCertificates generateTestCertificates() throws Exception
    {
        Security.addProvider(new BouncyCastleProvider());

        Path tempDir = Files.createTempDirectory("mongo-ssl-test");

        // Generate valid certificates
        CertificateInfo validCert = generateCertificate("testkey", 0, VALIDITY_DAYS);
        File validKeystore = saveKeystore(tempDir.resolve("test-keystore.jks").toFile(),
                "testkey", validCert);
        File validTruststore = saveTruststore(tempDir.resolve("test-truststore.jks").toFile(),
                "testcert", validCert.certificate);

        // Generate expired certificate (expired 1 day ago)
        CertificateInfo expiredCert = generateCertificate("expiredkey", -2, -1);
        File expiredKeystore = saveKeystore(tempDir.resolve("expired-keystore.jks").toFile(),
                "expiredkey", expiredCert);

        // Generate future certificate
        CertificateInfo futureCert = generateCertificate("futurekey",
                365 * 25,
                365 * 25 + VALIDITY_DAYS);
        File futureKeystore = saveKeystore(tempDir.resolve("future-keystore.jks").toFile(),
                "futurekey", futureCert);

        File emptyTruststore = createEmptyTruststore(tempDir.resolve("empty-truststore.jks").toFile());

        return new TestCertificates(
                validKeystore, validTruststore,
                expiredKeystore, futureKeystore,
                emptyTruststore, PASSWORD);
    }

    private static CertificateInfo generateCertificate(String commonName, int startDaysFromNow, int endDaysFromNow)
            throws Exception
    {
        KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
        keyGen.initialize(2048);
        KeyPair keyPair = keyGen.generateKeyPair();

        long now = System.currentTimeMillis();
        Date startDate = new Date(now + startDaysFromNow * 24L * 60L * 60L * 1000L);
        Date endDate = new Date(now + endDaysFromNow * 24L * 60L * 60L * 1000L);

        X500Name issuer = new X500Name("CN=" + commonName);
        BigInteger serialNumber = new BigInteger(64, new SecureRandom());

        ContentSigner contentSigner = new JcaContentSignerBuilder("SHA256withRSA")
                .build(keyPair.getPrivate());

        JcaX509v3CertificateBuilder certBuilder = new JcaX509v3CertificateBuilder(
                issuer, serialNumber, startDate, endDate, issuer, keyPair.getPublic());

        X509CertificateHolder certHolder = certBuilder.build(contentSigner);
        X509Certificate certificate = new JcaX509CertificateConverter()
                .setProvider("BC")
                .getCertificate(certHolder);

        return new CertificateInfo(keyPair, certificate);
    }

    private static File saveKeystore(File file, String alias, CertificateInfo certInfo) throws Exception
    {
        KeyStore keyStore = KeyStore.getInstance("JKS");
        keyStore.load(null, null);
        keyStore.setKeyEntry(alias, certInfo.keyPair.getPrivate(),
                PASSWORD.toCharArray(), new Certificate[]{certInfo.certificate});

        try (FileOutputStream fos = new FileOutputStream(file)) {
            keyStore.store(fos, PASSWORD.toCharArray());
        }

        log.debug("Generated keystore: {}", file.getAbsolutePath());
        return file;
    }

    private static File saveTruststore(File file, String alias, X509Certificate certificate) throws Exception
    {
        KeyStore trustStore = KeyStore.getInstance("JKS");
        trustStore.load(null, null);
        trustStore.setCertificateEntry(alias, certificate);

        try (FileOutputStream fos = new FileOutputStream(file)) {
            trustStore.store(fos, PASSWORD.toCharArray());
        }

        log.debug("Generated truststore: {}", file.getAbsolutePath());
        return file;
    }

    private static File createEmptyTruststore(File file) throws Exception
    {
        KeyStore emptyStore = KeyStore.getInstance("JKS");
        emptyStore.load(null, null);

        try (FileOutputStream fos = new FileOutputStream(file)) {
            emptyStore.store(fos, PASSWORD.toCharArray());
        }

        log.debug("Generated empty truststore: {}", file.getAbsolutePath());
        return file;
    }

    public static class TestCertificates
    {
        public final File validKeystore;
        public final File validTruststore;
        public final File expiredKeystore;
        public final File futureKeystore;
        public final File emptyTruststore;
        public final String password;

        public TestCertificates(File validKeystore, File validTruststore, File expiredKeystore,
                                File futureKeystore, File emptyTruststore, String password)
        {
            this.validKeystore = validKeystore;
            this.validTruststore = validTruststore;
            this.expiredKeystore = expiredKeystore;
            this.futureKeystore = futureKeystore;
            this.emptyTruststore = emptyTruststore;
            this.password = password;
        }
    }

    private static class CertificateInfo
    {
        public final KeyPair keyPair;
        public final X509Certificate certificate;

        public CertificateInfo(KeyPair keyPair, X509Certificate certificate)
        {
            this.keyPair = keyPair;
            this.certificate = certificate;
        }
    }
}
