package org.example.bigdata.realtime.utils;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.Serializable;
import java.util.Map;

public class RedisCache implements Serializable {

    //定义pool和单独的jedis
    private static JedisPool pool = null;
    private static Jedis executor = null;

    //连接池初始化参数
    private static final int port = 6379;
    private static final int timeout = 10 * 1000;
    private static final int maxIdle = 10;
    private static final int minIdle = 2;
    private static final int maxTotal = 20;

    //将初始化参数放到配置对象中
    private GenericObjectPoolConfig createConfig(){
        GenericObjectPoolConfig config = new GenericObjectPoolConfig();
        config.setMaxIdle(maxIdle);
        config.setMaxTotal(maxTotal);
        config.setMinIdle(minIdle);
        return config;
    }


    //获取jedis的连接
    public JedisPool connectRedisPool(String ip){
        if(null == pool){
            GenericObjectPoolConfig config = createConfig();
            pool = new JedisPool(config, ip, port, timeout);
        }
        return pool;
    }
    public Jedis connectRedis(String ip, int port, String auth){
        if(null == executor){
            executor = new Jedis(ip, port);
            executor.auth(auth);
        }
        return executor;
    }
    public static void main(String[] args) {
        String ip = "hadoop01";
        int port = 6379;
        String auth = "root";
        RedisCache cache = new RedisCache();
        JedisPool pool = cache.connectRedisPool(ip);
        Jedis jedis = pool.getResource();
        jedis.auth(auth);
        //========================================
/*        String productTable = "travel.dim_product1";
        String productID = "210602273";
        String pubV = jedis.hget(productTable, productID);
        System.out.println("redis.product =" + productID + " , pubValue=" + pubV);
        Map<String,String> redisPubData = jedis.hgetAll(productTable);
        System.out.println("redis.product =" + redisPubData);*/

        jedis.set("java_redis","nice flink");
        System.out.println(jedis.ping());

    }

}
