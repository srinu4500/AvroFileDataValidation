package com.avro.service;

import java.io.FileWriter;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.LocalTime;
import java.util.Random;

import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.avro.exception.InvalidNumberOfArgumentsException;
import com.avro.exception.MyCustomException;

public class JsonFileCreator {
	
	private static final Logger logger = Logger.getLogger(JsonFileCreator.class);
	
	public static void main(String[] args) throws InvalidNumberOfArgumentsException, MyCustomException, NoSuchAlgorithmException {
		String fileName = null;
		logger.info("creating json started"+LocalTime.now());
		long start = System.currentTimeMillis();
		if(args == null)
			throw new InvalidNumberOfArgumentsException("Need to pass Json file name");
		else if(args.length > 1)
			throw new InvalidNumberOfArgumentsException("Need to pass only fileName");
		else {
			fileName = args[0];
			JSONObject customerList = getDataObjectList();
			createJsonFile(fileName, customerList);
		}
        
        logger.info("Total Time Taken : "+(System.currentTimeMillis() - start)/1000 + " secs");
	}

	private static void createJsonFile(String fileName, JSONObject customerList) throws MyCustomException {
		//Write JSON file
        try (FileWriter file = new FileWriter(fileName)) {
 
            file.write(customerList.toJSONString());
            file.flush();
            logger.info("file created successfully"+LocalTime.now());
 
        } catch (IOException e) {
            logger.error(e.getMessage());
            throw new MyCustomException("IOException");
        }
	}

	@SuppressWarnings("unchecked")
	private static JSONObject getDataObjectList() throws NoSuchAlgorithmException {
		Random random = SecureRandom.getInstanceStrong();
		JSONArray customers = new JSONArray();
		for(int i=1; i<= 25; i++) {
		
			JSONObject customerDetails = new JSONObject();
			customerDetails.put("id", i);
			if(i%7 == 0)
				customerDetails.put("firstName", "TestUser"+i);
			else 
				customerDetails.put("firstName", "Test"+i);
			if(i%13 == 0)
				customerDetails.put("lastName", "LastUser"+i);
			else
				customerDetails.put("lastName", "LN"+i);
	        customerDetails.put("automatedEmail", (i%2 == 0)? true : false);
	        int age = random.nextInt(100);
	        customerDetails.put("age", age);	        
	        customerDetails.put("height", 175.5);
	        customerDetails.put("weight", 50.5);
	        customers.add(customerDetails);
		}
        JSONObject customerList = new JSONObject(); 
     
        customerList.put("customers", customers);
		return customerList;
	}
}