package com.bfm.app.ignite.meetup;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;



public class HelloWorldCache {

	@Test
	public void helloWorldCache() {

	    try (Ignite ignite = Ignition.start()) {

            // Create a cache
	    	IgniteCache<String, String> cache = ignite.getOrCreateCache("cache");

            // Put cache entry
	    	String key = "key";
	    	String val = "Hello";
	    	cache.put(key, val);

	        // Modify cache entry
    		cache.put(key, cache.get(key) + " World");

            // Read cache entry
	    	System.out.println(cache.get(key));
		}
	}



	@Test
	public void cacheTransaction() {

	    try (Ignite ignite = Ignition.start()) {

            // Create a cache
	    	CacheConfiguration<String, String> config = new CacheConfiguration<>("cachetx");
	    	config.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
	    	IgniteCache<String, String> cache = ignite.getOrCreateCache(config);

            // Put cache entry
	    	String key = "key";
	    	String val = "Hello";
	    	cache.put(key, val);

	        // Modify cache entry
	    	try (Transaction tx = ignite.transactions().txStart()) {
	    		cache.put(key, cache.get(key) + " World");
				tx.commit();
	    	}

            // Read cache entry
	    	System.out.println(cache.get(key));
		}
	}
}
