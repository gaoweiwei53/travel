package org.example.bigdata.realtime.utils;

import org.example.bigdata.realtime.flink.constant.QRealTimeConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Properties;


/**
 * 读取指定目录或者指定的properties文件
 */
public class PropertyUtil implements Serializable {
    //日誌打印對象
    private static Logger log = LoggerFactory.getLogger(PropertyUtil.class);
    /**
     * 读取资源文件
     * @param proPath
     * @return
     */
    public static Properties readProperties(String proPath){
        Properties properties = null;
        InputStream is = null;
        try{
            is = PropertyUtil.class.getClassLoader().getResourceAsStream(proPath);
            properties = new Properties();
            properties.load(is);
        }catch(IOException ioe){
            log.error("loadProperties4Redis:" + ioe.getMessage());
        }finally {
            try{
                if(null != is){
                    is.close();
                }}catch (Exception e){
                e.printStackTrace();
            }
        }
        return properties;
    }
    //测试
    public static void main(String[] args) {
        System.out.println(PropertyUtil.readProperties(QRealTimeConstant.KAFKA_CONSUMER_CONFIG_URL()).getProperty("bootstrap.servers"));
    }
}

