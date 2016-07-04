package com.ignite.examples;

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

	    Ignition.setClientMode(true);
	    try (Ignite ignite = Ignition.start()) {
			
            // Create a cache
	        CacheConfiguration<String, String> config = new CacheConfiguration<String, String>("cache");
	        config.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
	        IgniteCache<String, String> cache = ignite.getOrCreateCache(config);
	        
            // Put cache entry
	        String key = "key";
	        cache.put(key, "Hello");
	        
	        // Modify cache entry
	        try (Transaction tx = ignite.transactions().txStart()) {
    	        String val = cache.get(key);
    	        cache.put(key, val + " World");
    	        tx.commit();
	        }
	        
            // Read cache entry
            System.out.println(cache.get(key));
		}
	}
}
