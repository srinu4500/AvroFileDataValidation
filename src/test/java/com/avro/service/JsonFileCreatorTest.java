package com.avro.service;

import java.security.NoSuchAlgorithmException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.avro.exception.InvalidNumberOfArgumentsException;
import com.avro.exception.MyCustomException;

public class JsonFileCreatorTest {

	@BeforeClass
	public static void setUp() {
		System.out.println("started");
	}

	@Test
	public void testMainSuccess() throws InvalidNumberOfArgumentsException, MyCustomException, NoSuchAlgorithmException {
		System.out.println("testing");
		String[] args = {"customers.json"};
		JsonFileCreator.main(args);	
	}
	
	@Test(expected=InvalidNumberOfArgumentsException.class)
	public void noArgumentsPassException() throws InvalidNumberOfArgumentsException, MyCustomException, NoSuchAlgorithmException {
		String[] args = null;
		JsonFileCreator.main(args);
	}
	
	@Test(expected=InvalidNumberOfArgumentsException.class)
	public void invalidArgumentsPassException() throws InvalidNumberOfArgumentsException, MyCustomException, NoSuchAlgorithmException {
		String[] args = {"customer.json",""};
		JsonFileCreator.main(args);
	}
	
	@AfterClass
	public static void tearDown() {
		System.out.println("stopped");
	}
}
