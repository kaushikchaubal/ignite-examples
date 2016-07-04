package com.ignite.examples;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.junit.Test;



public class HelloWorldCompute {

	@Test
	public void helloWorldCompute() throws InterruptedException {
	    Ignition.setClientMode(true);
	    try (Ignite ignite = Ignition.start()) {
	        ignite.compute().broadcast(() -> System.out.println("Hello World"));
	        
	        Thread.sleep(20000);
	           ignite.compute().broadcast(() -> System.out.println("Hello World"));
	    }
	}
}
