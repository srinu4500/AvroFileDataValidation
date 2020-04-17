package com.avro.exception;

public class MyCustomException extends Exception{

	private static final long serialVersionUID = 1L;

	public MyCustomException(String message) {
        super(message);
    }
}
