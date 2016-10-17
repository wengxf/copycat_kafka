package org.apache.kafka.common;

public class UnknownCodecException extends RuntimeException{
	
	private static final long serialVersionUID = 1L;
	private String message;
	
	public UnknownCodecException(String message) {
		this.message = message;
	}
}
