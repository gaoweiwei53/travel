package org.example.bigdata.realtime.utils;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class CommonUtil {
    //---日期相关----------------------------------------------
    /**
     * 日期格式化
     */
    public static String formatDate4Timestamp(Long ct, String type) {
        SimpleDateFormat sdf = new SimpleDateFormat(type);
        String result = null;
        try {
            if (null != ct) {
                Calendar cal = Calendar.getInstance();
                cal.setTimeInMillis(ct);
                result = sdf.format(cal.getTime());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }
    /*
     * @param date
     * @Return 日期转换成string字符串
     *
     */
    public static String formatDate4Def(Date date) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String result = null;
        try {
            if (null != date) {
                result = sdf.format(date);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }
/**
 * MD5处理
 * @param key the key to hash (variable length byte array)
 * @return MD5 hash as a 32 character hex string.
 */
public static String getMD5AsHex(byte[] key) {
    return getMD5AsHex(key, 0, key.length);
}
    /**
     * MD5处理
     * @param key the key to hash (variable length byte array)
     * @param offset
     * @param length
     * @return MD5 hash as a 32 character hex string.
     */
    private static String getMD5AsHex(byte[] key, int offset, int length) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(key, offset, length);
            byte[] digest = md.digest();
            return new String(Hex.encodeHex(digest));
        } catch (NoSuchAlgorithmException e) {
// this should never happen unless the JDK is messed up.
            throw new RuntimeException("Error computing MD5 hash", e);
        }
    }
    /*
     * @Description
     * @param key
     * @Return 將redis的key中的.替換成_
     * @Exception
     *
     */
    public static String replaceRedisKey(String key) {
        String result = key;
        if(StringUtils.isNotEmpty(key)){
            result = key.replaceAll("\\.","_");
        }
        return result;
    }
    //測試
    public static void main(String[] args) {
        String key = "travel.dim_product1";
        String rs = replaceRedisKey(key);
        System.out.println(rs);
    }
}

