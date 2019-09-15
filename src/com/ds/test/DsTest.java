package com.ds.test;

import static org.junit.Assert.*;

import java.io.IOException;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import com.ds.config.DataStoreException;
import com.ds.main.DataStore;

public class DsTest {
	
	@Test
	public void insert() throws IOException, NumberFormatException, DataStoreException, JSONException{
		
		DataStore ds = new DataStore("check.txt");
		
		JSONObject obj = new JSONObject();
		obj.put("one", "1");
		ds.insert("test30", obj,30);
		
		
	}
	
	@Test
	public void read() throws IOException, NumberFormatException, DataStoreException, JSONException{
		
		DataStore ds = new DataStore("check.txt");
		String key = "test3";
		ds.get(key);
		assertEquals("" , ds.get(key));
	}
	
	
	@Test
	public void delete() throws IOException, NumberFormatException, DataStoreException{
		
		DataStore ds = new DataStore("check.txt");
		ds.delete("test30");
		
	}
	
	@Test
	public void checkTtl() throws IOException, NumberFormatException, DataStoreException{
		DataStore ds = new DataStore("check.txt");
		assertEquals(true, ds.removeExpiredValue());
	}
	
}
