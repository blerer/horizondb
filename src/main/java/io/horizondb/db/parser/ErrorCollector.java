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

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.misc.NotNull;
import org.antlr.v4.runtime.misc.Nullable;

/**
 * <code>ErrorListener</code> used to collect the error that occurred during the parsing
 * of an HQL query.
 * 
 * @author Benjamin
 *
 */
public final class ErrorCollector extends BaseErrorListener {

    /**
     * The error encounter during parsing.
     */
    private ParsingError error; 
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void syntaxError(@NotNull Recognizer<?, ?> recognizer,
                            @Nullable Object offendingSymbol,
                            int line,
                            int charPositionInLine,
                            @NotNull String msg,
                            @Nullable RecognitionException e) {
        
        this.error = new ParsingError(line, charPositionInLine, msg);
    }

    /**
     * Return the collected error.
     * 
     * @return the collected error or <code>null</code> if no error has occurred.
     */
    public ParsingError getError() {
        return this.error;
    }

    /**
     * Returns <code>true</code> if one error has occurred during the parsing.
     * 
     * @return <code>true</code> if one error has occurred during the parsing, <code>false</code>
     * otherwise.
     */
    public boolean hasError() {
        return this.error != null;
    }
    
    /**
     * Information a bout an error that occurred during parsing.
     */
    public static final class ParsingError {
        
        /**
         * The number of the line where the error occurred.
         */
        private final int lineNumber;
        
        /**
         * The position of the char where the error occurred.
         */
        private final int charPosition;
        
        /**
         * The error message.
         */
        private final String message;

        /**
         * @param lineNumber
         * @param charPosition the position of the char where the error occurred
         * @param message
         */
        public ParsingError(int lineNumber, int charPosition, String message) {
            
            this.lineNumber = lineNumber;
            this.charPosition = charPosition;
            this.message = message;
        }

        /**
         * Returns the line number.
         * 
         * @return the line number.
         */
        public int getLineNumber() {
            return this.lineNumber;
        }

        /**
         * Returns the position of the char where the error occurred.
         * @return the position of the char where the error occurred
         */
        public int getCharPosition() {
            return this.charPosition;
        }

        /**
         * Returns the error message.
         * @return the error message.
         */
        public String getMessage() {
            return this.message;
        }
    }
}
