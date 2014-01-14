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
package io.horizondb.model.fields;

/**
 * @author Benjamin
 *
 */
public class TypeConversionException extends RuntimeException {


	/**
	 * 
	 */
    private static final long serialVersionUID = 5221495691368471547L;


	/**
	 * @param message
	 */
	public TypeConversionException(String message) {
		super(message);
	}


	/**
	 * @param message
	 * @param cause
	 */
	public TypeConversionException(String message, Throwable cause) {
		super(message, cause);
	}


}
