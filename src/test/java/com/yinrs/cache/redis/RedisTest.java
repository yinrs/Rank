package com.yinrs.cache.redis;

public class RedisTest {
    public static void main(String[] args) {
        RedisManager.I.init();
        RedisManager.I.set("mykey","myval",60);
        String myval = RedisManager.I.get("mykey", String.class);
        System.out.println(myval);
    }
}
