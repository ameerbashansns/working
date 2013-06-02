package com.liyaqat.hbase.dao.exception;

public class HBaseCrudException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7950371593797410624L;

	public HBaseCrudException() {
		// TODO Auto-generated constructor stub
	}

	public HBaseCrudException(String message) {
		super(message);
		// TODO Auto-generated constructor stub
	}

	public HBaseCrudException(Throwable cause) {
		super(cause);
		// TODO Auto-generated constructor stub
	}

	public HBaseCrudException(String message, Throwable cause) {
		super(message, cause);
		// TODO Auto-generated constructor stub
	}

	public HBaseCrudException(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
		// TODO Auto-generated constructor stub
	}

}
