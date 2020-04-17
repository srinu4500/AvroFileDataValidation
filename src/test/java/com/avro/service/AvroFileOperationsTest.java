package com.avro.service;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.avro.exception.InvalidNumberOfArgumentsException;
import com.avro.exception.MyCustomException;
import com.avro.model.CustomerModel;

public class AvroFileOperationsTest {

	private String configFile = "src/test/resources/config.properties";
	private String inputJsonFile = "customers.json";
	
	@BeforeClass
	public static void setUp() {
		System.out.println("started");
	}

	@Test
	public void argumenstsConstructor() {
		AvroFileOperations ao = new AvroFileOperations("a","b","c");
		assertNotNull(ao);
	}
	
	@Test
	public void defaultConstructor() {
		AvroFileOperations ao = new AvroFileOperations();
		assertNotNull(ao);
	}
	
	@Test
	public void successFlow() {
		AvroFileOperations ao = new AvroFileOperations(inputJsonFile,"config.properties",".");
		assertNotNull(ao);
		
	}
	
	@Test
	public void successAllFlows() throws InvalidNumberOfArgumentsException, MyCustomException, IOException {
		AvroFileOperations ao = new AvroFileOperations();
		String[] args = {"src/main/resources/customers.json","src/main/resources/config.properties","."};
		ao.main(args);
		assertNotNull(ao);
		
	}
	
	@Test
	public void readJsonFile() throws MyCustomException {
		AvroFileOperations ao = new AvroFileOperations("src/test/resources/customers.json",configFile,".");
		CustomerModel s = ao.readJsonFile();
		System.out.println(s.getCustomers().size());
		assertEquals(3, s.getCustomers().size());
	}
	
	@Test(expected=MyCustomException.class)
	public void fileNotAccessibleException() throws MyCustomException {
		AvroFileOperations ao = new AvroFileOperations("src/test/resources/customers.xml",configFile,".");
		ao.readJsonFile();
	}
	@Test
	public void jsonMappingException() {
		AvroFileOperations ao = new AvroFileOperations("src/test/resources/jsonmapping.json",configFile,".");
		try {
			ao.readJsonFile();
		} catch (MyCustomException e) {
			assertEquals("JsonMapping Exception", e.getMessage());
		}
	}
	
	@Test
	public void createAvroFile() throws MyCustomException {
		AvroFileOperations ao = new AvroFileOperations("src/test/resources/customers.json",configFile,".");
		CustomerModel s = ao.readJsonFile();
		assertEquals(3, s.getCustomers().size());
		
	}
	
	@Test(expected=InvalidNumberOfArgumentsException.class)
	public void invalidArgumentsPassException() throws InvalidNumberOfArgumentsException, MyCustomException, IOException {
		String[] args = {inputJsonFile,configFile};
		AvroFileOperations.main(args);
	}
	
	@Test
	public void invalidArgumentsPassExceptionMessageCheck() throws InvalidNumberOfArgumentsException {
		String[] args = {inputJsonFile,configFile};
		try {
			AvroFileOperations.main(args);
		} catch (Exception e) {
			System.out.println(e.getMessage());
			assertEquals("Need to pass 3 arguments", e.getMessage());
		}
	}

	@AfterClass
	public static void tearDown() {
		System.out.println("stopped");
	}
}
