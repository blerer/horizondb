/**
 * Copyright 2013 Benjamin Lerer
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.horizondb.client;

import io.horizondb.model.Error;

/**
 * @author Benjamin
 *
 */
public class HorizonDBException extends RuntimeException {

	/**
	 * 
	 */
    private static final long serialVersionUID = 3844334412343282479L;

	/**
	 * @param message
	 */
	public HorizonDBException(Error error) {
		super(new StringBuilder().append("[ERROR: ")
		                         .append(error.getCode())
		                         .append("] ")
		                         .append(error.getMessage())
		                         .toString());
	}
    
	/**
	 * @param message
	 */
	public HorizonDBException(String message) {
		super(message);
	}

	/**
	 * @param message
	 * @param cause
	 */
	public HorizonDBException(String message, Throwable cause) {
		super(message, cause);
	}
}
