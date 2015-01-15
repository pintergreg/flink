package org.apache.flink.streaming.api.ft.layer.util;

import java.io.Serializable;

public class InvalidTopologyException extends RuntimeException implements Serializable {
	private static final long serialVersionUID = 1L;
	/** The root cause throwable */
	private final Throwable throwable;

	/**
	 * Constructs a new <code>InvalidTopologyException</code>.
	 */
	public InvalidTopologyException() {
		super();
		throwable = null;
	}

	/**
	 * Construct a new <code>InvalidTopologyException</code>.
	 * 
	 * @param message
	 *            the detail message for this exception
	 */
	public InvalidTopologyException(String message) {
		this(message, null);
	}

	/**
	 * Construct a new <code>InvalidTopologyException</code>.
	 * 
	 * @param message
	 *            the detail message for this exception
	 * @param exception
	 *            the root cause of the exception
	 */
	public InvalidTopologyException(String message, Throwable exception) {
		super(message);
		throwable = exception;
	}

	/**
	 * Gets the root cause of the exception.
	 *
	 * @return the root cause
	 */
	public final Throwable getCause() {
		return throwable;
	}
}
