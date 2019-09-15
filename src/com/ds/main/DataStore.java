package com.ds.main;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.json.JSONException;
import org.json.JSONObject;

import com.ds.config.DataStoreException;

public class DataStore {

	/*
	 * TODO : JSON Object value size 
	 * File size
	 */
	
	
	public static final int timeTolive = 86400;

	private String file = "";

	public DataStore(String path, String fileName) throws IOException, DataStoreException {

		File dsFile = new File(path, fileName);
		
		if (!dsFile.createNewFile())
			throw new DataStoreException("File already exists");
	}

	public  DataStore(String fileName) throws IOException {
	File dsFile = new File(System.getProperty("user.dir"), fileName);
/*
		RandomAccessFile randomAccessFile = new RandomAccessFile(dsFile,"rw");
		
		long l = 9999999990L;
		
		randomAccessFile.setLength(1024L * 1024L);
		
		randomAccessFile.close();*/
		dsFile.createNewFile();
		this.file = fileName;
	}

	/*
	 * Function to inset the entries
	 */

	public  synchronized  void  insert(String key, JSONObject value, int ttl)
			throws IOException, NumberFormatException, DataStoreException {

		

		int maxLength = (key.length() < 32) ? key.length() : 32;
		key = key.substring(0, maxLength);

		removeExpiredValue();
		
		System.out.println(checkIfKeyExists(key));
		if (checkIfKeyExists(key))
			throw new DataStoreException("Duplicate Key . Kindly try with different key");
		else {
			BufferedWriter writer = new BufferedWriter(new FileWriter(file, true));
			writer.write(key + "|" + value + "|" + Long.toString(new Date().getTime()) + "|" + ttl);
			writer.write(System.lineSeparator());
			writer.close();
		}

	}
	
	public synchronized  void insert(String key, JSONObject obj) throws NumberFormatException, IOException, DataStoreException{
		insert(key,obj,DataStore.timeTolive);
	}
	
	
	/*
	 * Check whether the key present in the file
	 */

	public boolean checkIfKeyExists(String key) throws IOException {

		BufferedReader reader = null;

		try {
			File inputFile = new File(file);

			reader = new BufferedReader(new FileReader(inputFile));

			String line;
			while ((line = reader.readLine()) != null) {

				
				String[] values = line.trim().split("\\|");
				if (values[0].equalsIgnoreCase(key))
					return true;

			}
			reader.close();
		} finally {
			reader.close();

		}

		return false;
	}

	
	/*
	 * Function to remove the expired values and delete the values based on flag
	 */

	public boolean removeExpiredValue()
			throws NumberFormatException, IOException, DataStoreException {

		BufferedReader fileReader = null;
		BufferedWriter tmpWriter = null;

		try {

			File tempFile = new File("tmp.txt");
			File inputFile = new File(file);
			fileReader = new BufferedReader(new FileReader(inputFile));
			tmpWriter = new BufferedWriter(new FileWriter(tempFile));

			String line;


				while ((line = fileReader.readLine()) != null ) {

				if(line.trim().equals(""))
					break;

					String[] values = line.split("\\|");
					
					long duration = TimeUnit.MILLISECONDS.toSeconds((new Date().getTime()) - Long.parseLong(values[2]));
					
					if (duration > Integer.parseInt(values[3])) 
						continue;
					
					tmpWriter.write(line + System.lineSeparator());
				}
				
				tmpWriter.close();
				fileReader.close();
				inputFile.delete();
				
				tempFile.renameTo(inputFile);

				return true;
			
		} finally {

			tmpWriter.close();
			fileReader.close();
		}
	}

	/*
	 * Function to retrive the values
	 */
	public synchronized JSONObject get(String key) throws NumberFormatException, IOException, DataStoreException, JSONException {
		
		BufferedReader fileReader = null;

		try {
			
			removeExpiredValue();
			
			File inputFile = new File(file);
			fileReader = new BufferedReader(new FileReader(inputFile));
			String line;

			while ((line = fileReader.readLine()) != null) {

				String[] values = line.split("\\|");
				if (values[0].equalsIgnoreCase(key)) 
					return new JSONObject(values[1]);
				

			}

			fileReader.close();
		} finally {

			fileReader.close();

		}

		throw new DataStoreException("key not found");

	}
	
	/*
	 * Deletion
	 */

	public synchronized void delete(String key) throws IOException, NumberFormatException, DataStoreException {

		removeExpiredValue();
		
		if (checkIfKeyExists(key)) {
			
			BufferedReader fileReader = null;
			BufferedWriter tmpWriter = null;

			try {

				File tempFile = new File("tmp.txt");
				File inputFile = new File(file);
				fileReader = new BufferedReader(new FileReader(inputFile));
				tmpWriter = new BufferedWriter(new FileWriter(tempFile));

				String line;
			
			while ((line = fileReader.readLine()) != null) {

				String[] values = line.split("\\|");

				if (values[0].equalsIgnoreCase(key)) continue;
	
				tmpWriter.write(line + System.lineSeparator());
			}
			
			tmpWriter.close();
			fileReader.close();
			inputFile.delete();

			tempFile.renameTo(inputFile);
			
			}finally{
				tmpWriter.close();
				fileReader.close();
			}

			
			
		} else
			throw new DataStoreException("Key not found");

	}


}
