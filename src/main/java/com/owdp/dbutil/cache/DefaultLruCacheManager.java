package com.owdp.dbutil.cache;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * DbUtils库默认的缓存管理器，基于LRU Cache来实现。
 */
public final class DefaultLruCacheManager implements CacheManager{

    private AndroidLruCache<String, Cache> caches ;

    private int size;

    public DefaultLruCacheManager(int size) {
        if(size <= 0){
            throw new IllegalArgumentException("cache size <= 0!");
        }
        this.caches = new AndroidLruCache<String, Cache>(size);
        this.size = size;
    }

    @Override
    public Cache getCache(String name) {
        Cache cache = caches.get(name);
        if(cache == null){
            cache = new Cache(new AndroidLruCache<String, Object>(size),name);
            caches.put(name,cache);
        }
        return cache;
    }

    @Override
    public Collection<String> getNames() {
        return caches.map().keySet();
    }

    @Override
    public void clearAll() {
        for(Cache cache : caches.map().values()){
            if(cache != null){
                cache.clear();
            }
        }
    }

    private static class Cache implements com.owdp.dbutil.cache.Cache{

        private AndroidLruCache<String,Object> androidLruCache;
        private String name;

        public Cache(AndroidLruCache<String,Object> androidLruCache,String name) {
            this.androidLruCache = androidLruCache;
            this.name = name;
        }

        @Override
        public void clear() {
            androidLruCache.evictAll();
        }

        @Override
        public void remove(String key) {
            androidLruCache.remove(key);
        }

        @Override
        public Object get(String key,Class c) {
            return androidLruCache.get(key);
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public void put(String key, Object value) {
            androidLruCache.put(key,value);
        }
    }
}
