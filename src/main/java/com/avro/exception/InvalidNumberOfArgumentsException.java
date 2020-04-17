package com.avro.exception;

public class InvalidNumberOfArgumentsException extends Exception {
   
	private static final long serialVersionUID = 4116034870955836989L;

	public InvalidNumberOfArgumentsException(String message) {
        super(message);
    }
}
