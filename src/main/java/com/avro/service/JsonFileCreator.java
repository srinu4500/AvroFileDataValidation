package com.avro.service;

import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalTime;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class JsonFileCreator {

	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		System.out.println("creating json started"+LocalTime.now());
		long start = System.currentTimeMillis();

		//Add customers to list
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
	        customerDetails.put("age", (int)(Math.random() * ((100 - 1) + 1)) + 1);
	        if(i%2 == 0)
	        	customerDetails.put("automatedEmail", true);
	        else 
	        	customerDetails.put("automatedEmail", false);
	        customerDetails.put("height", 175.5);
	        customerDetails.put("weight", 50.5);
	        customers.add(customerDetails);
		}
        JSONObject customerList = new JSONObject(); 
     
        customerList.put("customers", customers);
        
        //Write JSON file
        try (FileWriter file = new FileWriter("customers.json")) {
 
            file.write(customerList.toJSONString());
            file.flush();
            System.out.println("file created successfully"+LocalTime.now());
 
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
        System.out.println("Total Time Taken : "+(System.currentTimeMillis() - start)/1000 + " secs");
	}
	

}
