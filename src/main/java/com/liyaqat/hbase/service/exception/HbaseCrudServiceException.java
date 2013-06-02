package com.liyaqat.hbase.service.exception;

public class HbaseCrudServiceException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 9060911043182302743L;

	public HbaseCrudServiceException() {
		// TODO Auto-generated constructor stub
	}

	public HbaseCrudServiceException(String message) {
		super(message);
		// TODO Auto-generated constructor stub
	}

	public HbaseCrudServiceException(Throwable cause) {
		super(cause);
		// TODO Auto-generated constructor stub
	}

	public HbaseCrudServiceException(String message, Throwable cause) {
		super(message, cause);
		// TODO Auto-generated constructor stub
	}

	public HbaseCrudServiceException(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
		// TODO Auto-generated constructor stub
	}

}
