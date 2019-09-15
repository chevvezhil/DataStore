package com.ds.config;

import java.lang.instrument.Instrumentation;

public class ObjectSizeFectcher {
	
	 private static Instrumentation instrumentation;

	    public static void premain(String args, Instrumentation inst) {
	        instrumentation = inst;
	    }

	    public static long getObjectSize(Object o) {
	    	
	    	if (instrumentation == null) {
	            throw new IllegalStateException("Agent not initialized.");
	        }
	        return instrumentation.getObjectSize(o);
	    }
}
