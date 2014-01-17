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
package io.horizondb.db;

import io.horizondb.model.ErrorCodes;

import java.util.Arrays;

import org.apache.commons.lang.Validate;

import static org.apache.commons.lang.StringUtils.containsAny;

/**
 * Utility methods to check names restrictions.
 * 
 * @author Benjamin
 * 
 */
public final class Names {

    /**
     * The characters that must not be within a database name.
     */
    private static final char[] DATABASE_NAME_INVALID_CHARACTERS = new char[] { '/', '\\', '.', '"', '*', '<', '>',
            ':', '|', '?' };

    /**
     * The characters that must not be within a time series name.
     */
    private static final char[] TIMESERIES_NAME_INVALID_CHARACTERS = new char[] { '/', '\\', '.', '"', '*', '<', '>',
            ':', '|', '?' };

    /**
     * Checks that the specified name is a valid database name.
     * 
     * @param name the database name to check.
     * @throws HorizonDBException if the database name is not valid.
     */
    public static void checkDatabaseName(String name) throws HorizonDBException {

        Validate.notEmpty(name, "Database names must not be empty.");

        if (containsWhiteSpace(name)) {

            String msg = "Database names must not contains any whitespace characters (Database name: '" + name + "').";

            throw new HorizonDBException(ErrorCodes.INVALID_DATABASE_NAME, msg);
        }

        if (containsAny(name, DATABASE_NAME_INVALID_CHARACTERS)) {

            String msg = "Database names must not contains any of the following characters: "
                    + Arrays.toString(DATABASE_NAME_INVALID_CHARACTERS) + " (Database name: " + name + ").";

            throw new HorizonDBException(ErrorCodes.INVALID_DATABASE_NAME, msg);
        }
    }

    /**
     * Checks that the specified name is a valid time series name.
     * 
     * @param name the time series name to check.
     * @throws HorizonDBException if the time series name is not valid.
     */
    public static void checkTimeSeriesName(String name) throws HorizonDBException {

        Validate.notEmpty(name, "Time series names must not be empty.");

        if (containsWhiteSpace(name)) {

            String msg = "Time series names must not contains any whitespace characters.";

            throw new HorizonDBException(ErrorCodes.INVALID_TIMESERIES_NAME, msg);
        }

        if (containsAny(name, TIMESERIES_NAME_INVALID_CHARACTERS)) {

            String msg = "Time series names must not contains any of the following characters: "
                    + Arrays.toString(DATABASE_NAME_INVALID_CHARACTERS) + " (Time series name: " + name + ").";

            throw new HorizonDBException(ErrorCodes.INVALID_TIMESERIES_NAME, msg);
        }
    }

    /**
     * Returns <code>true</code> if the specified name contains a white space character, <code>false</code> otherwise.
     * 
     * @param name the name to check.
     * @return <code>true</code> if the specified name contains a white space character, <code>false</code> otherwise.
     */
    private static boolean containsWhiteSpace(String name) {

        for (int i = 0, m = name.length(); i < m; i++) {

            if (Character.isWhitespace(name.charAt(i))) {
                return true;
            }
        }

        return false;
    }

    /**
     * Must not be instantiated.
     */
    private Names() {

    }
}
