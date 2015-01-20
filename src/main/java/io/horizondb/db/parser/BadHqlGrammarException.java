/**
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
package io.horizondb.db.parser;

import io.horizondb.db.HorizonDBException;
import io.horizondb.model.ErrorCodes;

/**
 * Exception thrown by the HQL parser when an HQL query is invalid.
 */
public final class BadHqlGrammarException extends HorizonDBException {

    /**
     * Serial UID
     */
    private static final long serialVersionUID = 8906102712317030814L;

    public BadHqlGrammarException(String query, int lineNumber, int charPosition, String message) {
        super(ErrorCodes.INVALID_QUERY, errorMessage(query, lineNumber, charPosition, message));
    }

    public BadHqlGrammarException(String message) {
        super(ErrorCodes.INVALID_QUERY, message);
    }

    /**
     * Creates the exception error message.
     * 
     * @param query the invalid query
     * @param lineNumber the line where the error occurred
     * @param charPosition the char position within the line
     * @return error message
     */
    private static String errorMessage(String query, int lineNumber, int charPosition, String message) {
        
        return new StringBuilder().append("The query: '")
                                  .append(query)
                                  .append("' has an error in line ")
                                  .append(lineNumber)
                                  .append(" character ")
                                  .append(charPosition)
                                  .append(": ")
                                  .append(message)
                                  .toString();
    }

}
