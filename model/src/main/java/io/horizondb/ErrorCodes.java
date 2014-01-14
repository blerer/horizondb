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
package io.horizondb;

/**
 * The error codes.
 * 
 * @author Benjamin
 *
 */
public final class ErrorCodes {

	/**
	 * A message with an unknown operation code has been received.
	 */
	public static final int UNKNOWN_OPERATION_CODE = 100; 
	
	/**
	 * An internal error has occurred.
	 */
	public static final int INTERNAL_ERROR = 101; 
	
	/**
	 * A database with the same name already exists.
	 */
	public static final int DUPLICATE_DATABASE = 102; 
	
	/**
	 * An unknown database access has been requested.
	 */
	public static final int UNKNOWN_DATABASE = 103; 
		
	/**
	 * A time series with the same name already exists.
	 */
	public static final int DUPLICATE_TIMESERIES = 104; 
	
	/**
	 * An unknown time series access has been requested.
	 */
	public static final int UNKNOWN_TIMESERIES = 105; 
	
	/**
	 * The database name is invalid.
	 */
	public static final int INVALID_DATABASE_NAME = 106; 
	
	/**
	 * The time series name is invalid.
	 */
	public static final int INVALID_TIMESERIES_NAME = 107; 
	
	/**
	 * The record set send by the client is invalid. 
	 * Its first record is a delta and should be a full record.
	 */
	public static final int INVALID_RECORD_SET = 108; 
	
	/**
	 * The database configuration is not valid.
	 */
	public static final int INVALID_CONFIGURATION = 109; 
}
