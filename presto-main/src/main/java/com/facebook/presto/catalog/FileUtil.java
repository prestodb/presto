package com.facebook.presto.catalog;
/*
 * ----------------------------------------------------------------------
 * Copyright © 2014-2021 China Mobile (SuZhou) Software Technology Co.,Ltd.
 *
 * The programs can not be copied and/or distributed without the express
 * permission of China Mobile (SuZhou) Software Technology Co.,Ltd.
 *
 * ----------------------------------
 */

import com.facebook.airlift.log.Logger;

import java.io.*;
import java.util.*;

/**
 * @author ahern
 * @date 2021/10/12 14:19
 * @since 1.0
 */
public class FileUtil {


    private static final Logger log = Logger.get(FileUtil.class);

    /**
     * 根据Key读取Value信息
     *
     * @param filePath 文件地址
     * @param key      key
     * @return value
     */
    public static String getValueByKey(String filePath, String key) {
        Properties pps = new Properties();
        try (InputStream in = new BufferedInputStream(new FileInputStream(filePath))) {
            pps.load(in);
            return pps.getProperty(key);
        } catch (IOException e) {
            log.error("get value error", e);
            return null;
        }
    }

    /**
     * 读取全部Properties文件
     *
     * @param filePath 文件辣眼睛
     * @return Map
     * @throws IOException IO异常
     */
    public static Map<String, String> getAllProperties(String filePath) throws IOException {
        Map<String, String> result = new HashMap<>();
        Properties properties = new Properties();
        InputStream in = new BufferedInputStream(new FileInputStream(filePath));
        properties.load(in);
        Enumeration en = properties.propertyNames();
        while (en.hasMoreElements()) {
            String key = en.nextElement().toString();
            result.put(key, properties.getProperty(key));
        }
        return result;
    }

    /**
     * 写Properties文件
     *
     * @param filePath 文件
     * @param pKey     key
     * @param pValue   value
     * @throws IOException IO异常
     */
    public static void writeProperties(String filePath, String pKey, String pValue) throws IOException {
        Properties properties = new Properties();
        try (FileWriter fw = new FileWriter(filePath, true); InputStream in = new FileInputStream(filePath)) {
            //从输入流中读取属性列表（键和元素对）
            properties.load(in);
            //调用 Hashtable 的方法 put。使用 getProperty 方法提供并行性。
            //强制要求为属性的键和值使用字符串。返回值是 Hashtable 调用 put 的结果。
            OutputStream out = new FileOutputStream(filePath);
            properties.setProperty(pKey, pValue);
            //以适合使用 load 方法加载到 Properties 表中的格式，
            //将此 Properties 表中的属性列表（键和元素对）写入输出流
            properties.store(out, "update " + pKey + " name");
        }
    }

    /**
     * 写Properties文件
     *
     * @param filePath 文件路径
     * @param map      键值对
     * @param append   是否拼接
     * @throws IOException IO异常
     */
    public static void writeProperties(String filePath, Map<String, String> map, boolean append) throws IOException {
        Properties properties = new Properties();
        try (FileWriter fw = new FileWriter(filePath, append)) {
            properties.putAll(map);
            properties.store(fw, filePath);
        }
    }

    /**
     * 写文件
     *
     * @param filePath 文件路径
     * @param content  文件内容
     * @param append   是否进行拼接
     * @throws IOException IO异常
     */
    public static void writeFile(String filePath, String content, boolean append) throws IOException {
        try (FileWriter fw = new FileWriter(filePath, append)) {
            fw.write(content);
        }
    }

    /**
     * 删除文件(若被删除的文件是目录，则删除该目录下所有文件)
     *
     * @param filePath 文件路径
     */
    public static void delFile(String filePath) {
        File file = new File(filePath);
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            if (null == files) {
                file.delete();
            }
            Arrays.stream(files).forEach(f -> delFile(f.getPath()));
        }
        file.delete();
    }
}
