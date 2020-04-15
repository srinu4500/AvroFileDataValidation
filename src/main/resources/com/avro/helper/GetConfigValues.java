package com.avro.helper;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class GetConfigValues {
	String result = "";
	InputStream inputStream;
 
	public String getValueByKey(String key) {
		
		Properties prop = new Properties();
		String propFileName = "config.properties";
		String response = "";
		try {
			inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);
	
			if (inputStream != null) {
				prop.load(inputStream);
				if(key != null)
					response = prop.getProperty(key);
			} else {
				throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
			}
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		return response;
	}
	/*public String getKeyValue(String key) {
		System.out.println("*********");
		
		try (InputStream input = new FileInputStream("config.properties")) {

            Properties prop = new Properties();

            // load a properties file
            prop.load(input);

            // get the property value and print it out
            System.out.println(prop.getProperty(key));
            result = prop.getProperty(key);
		} catch (IOException ex) {
            ex.printStackTrace();
        }
		
		return result;
	}*/
}
