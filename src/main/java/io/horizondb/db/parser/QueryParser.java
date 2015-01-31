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

import io.horizondb.db.Configuration;
import io.horizondb.db.HorizonDBException;
import io.horizondb.db.databases.DatabaseManager;
import io.horizondb.db.parser.ErrorHandler.SyntaxException;
import io.horizondb.db.parser.HqlParser.StatementsContext;
import io.horizondb.db.parser.builders.MsgBuilderDispatcher;
import io.horizondb.io.serialization.Serializable;
import io.horizondb.model.protocol.HqlQueryPayload;
import io.horizondb.model.protocol.Msg;

import java.io.IOException;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The parser used to parse the HQL queries.
 */
public final class QueryParser {

    /**
     * The logger.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryParser.class);
    
    /**
     * Parses the specified query string of the specified message and return the corresponding low level message.
     * 
     * @param configuration the database configuration
     * @param databaseManager the database manager
     * @param msg the query message
     * @return the low level message corresponding to the specified <code>query</code>.
     * @throws HorizonDBException if a problem occurs while parsing the query.
     * @throws IOException if an I/O problem occurs while parsing the query
     */
    public static <T extends Serializable> Msg<T> parse(Configuration configuration, 
                                                        DatabaseManager databaseManager,
                                                        Msg<HqlQueryPayload> msg) 
            throws HorizonDBException, IOException  {
              
        HqlQueryPayload payload = msg.getPayload();
        
        String databaseName = payload.getDatabaseName();
        String query = payload.getQuery();

        LOGGER.debug("parsing query: [{}] for database {}.", query, databaseName);
        try {
            final ErrorHandler errorHandler = new ErrorHandler();

            CharStream stream = new ANTLRInputStream(query);
            HqlLexer lexer = new HqlLexer(stream);
            lexer.removeErrorListeners();
            lexer.addErrorListener(errorHandler);
            CommonTokenStream tokens = new CommonTokenStream(lexer);

            HqlParser parser = new HqlParser(tokens);
            parser.removeErrorListeners();
            parser.addErrorListener(errorHandler);

            StatementsContext statements = parser.statements();

            ParseTreeWalker walker = new ParseTreeWalker();
            MsgBuilder msgBuilder = new MsgBuilderDispatcher(configuration,
                                                             databaseManager,
                                                             msg.getHeader(),
                                                             databaseName);
            walker.walk(msgBuilder, statements);

            return (Msg<T>) msgBuilder.build();
        
        } catch (SyntaxException e) {
            
            throw new BadHqlGrammarException(query, 
                                             e.getLineNumber(), 
                                             e.getCharPosition(), 
                                             e.getMessage());
        }
    }
}
