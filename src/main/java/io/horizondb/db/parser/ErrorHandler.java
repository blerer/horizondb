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
 * <code>ErrorListener</code> used to convert the syntax errors of an HQL query into <code>SyntaxException</code>.
 */
public final class ErrorHandler extends BaseErrorListener {

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

        throw new SyntaxException(line, charPositionInLine, msg);
    }

    /**
     * Information about an error that occurred during parsing.
     */
    public static final class SyntaxException extends RuntimeException {
        
        /**
         * The serial version UID.
         */
        private static final long serialVersionUID = -2783921035310035769L;

        /**
         * The number of the line where the error occurred.
         */
        private final int lineNumber;
        
        /**
         * The position of the char where the error occurred.
         */
        private final int charPosition;

        /**
         * @param lineNumber
         * @param charPosition the position of the char where the error occurred
         * @param message
         */
        public SyntaxException(int lineNumber, int charPosition, String message) {
            
            super(message);
            this.lineNumber = lineNumber;
            this.charPosition = charPosition;
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
    }
}
