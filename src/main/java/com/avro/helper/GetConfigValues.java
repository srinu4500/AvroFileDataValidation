package com.avro.helper;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class GetConfigValues {
	
	public String getValueByKey(String key, String propFileName) {
		Properties prop = new Properties();
		String response = "";
		try {
			// load a properties file for reading
			prop.load(new FileInputStream(propFileName));

			if (key != null)
				response = prop.getProperty(key);

		} catch (IOException ex) {
			System.out.println(ex.getMessage());
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
		return response;
	}
}