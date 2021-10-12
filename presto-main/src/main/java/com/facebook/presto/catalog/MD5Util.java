package com.facebook.presto.catalog;

import com.sun.org.apache.xerces.internal.impl.dv.util.Base64;

import javax.crypto.KeyGenerator;
import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

/**
 * @author ahern
 * @date 2021/10/12 10:46
 * @since 1.0
 */
public class MD5Util {

    public volatile static MD5Util me;

    private MD5Util(){

    }

    /**
     * 单例
     *
     * @return MD5Util
     */
    public static MD5Util getInstance() {
        if (me == null) {
            synchronized (MD5Util.class) {
                if (me == null) {
                    me = new MD5Util();
                }
            }
        }
        return me;
    }

    public static final String MD5 = "MD5";
    public static final String HmacMD5 = "HmacMD5";

    /**
     * 编码格式；默认使用uft-8
     */
    private static final String CHARSET = StandardCharsets.UTF_8.name();

    /**
     * 使用MessageDigest进行单向加密（无密码）
     *
     * @param res       被加密的文本
     * @param algorithm 加密算法名称
     * @return
     */
    private String messageDigest(String res, String algorithm) {
        try {
            MessageDigest md = MessageDigest.getInstance(algorithm);
            byte[] resBytes = CHARSET == null ? res.getBytes() : res.getBytes(CHARSET);
            return base64(md.digest(resBytes));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private String base64(byte[] res) {
        return Base64.encode(res);
    }


    /**
     * md5加密算法进行加密（不可逆）
     *
     * @param res 需要加密的原文
     * @return
     */
    public String MD5(String res) {
        return messageDigest(res, MD5);
    }

    /**
     * md5加密算法进行加密（不可逆）
     *
     * @param res 需要加密的原文
     * @param key 秘钥
     * @return
     */
    public String MD5(String res, String key) {
        return keyGeneratorMac(res, HmacMD5, key);
    }

    /**
     * 使用KeyGenerator进行单向/双向加密（可设密码）
     *
     * @param res       被加密的原文
     * @param algorithm 加密使用的算法名称
     * @param key       加密使用的秘钥
     * @return
     */
    private String keyGeneratorMac(String res, String algorithm, String key) {
        try {
            SecretKey sk = null;
            if (key == null) {
                KeyGenerator kg = KeyGenerator.getInstance(algorithm);
                sk = kg.generateKey();
            } else {
                byte[] keyBytes = CHARSET == null ? key.getBytes() : key.getBytes(CHARSET);
                sk = new SecretKeySpec(keyBytes, algorithm);
            }
            Mac mac = Mac.getInstance(algorithm);
            mac.init(sk);
            byte[] result = mac.doFinal(res.getBytes());
            return base64(result);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
