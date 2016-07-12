package com.bfm.app.ignite.meetup;

import org.junit.Test;



public class HelloWorldCompute {

	@Test
	public void helloWorldCompute() throws InterruptedException {

		try (Ignite ignite = Ignition.start()) {
			ignite.compute().broadcast(() -> System.out.println("Hello World"));
		}
	}
}
