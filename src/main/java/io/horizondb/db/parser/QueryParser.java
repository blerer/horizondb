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
import io.horizondb.db.Query;
import io.horizondb.db.parser.ErrorCollector.ParsingError;
import io.horizondb.db.parser.HqlParser.StatementsContext;
import io.horizondb.db.parser.builders.QueryBuilderDispatcher;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The parser used to parse the HQL queries.
 * 
 * @author Benjamin
 */
public final class QueryParser {

    /**
     * The logger.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryParser.class);
    
    /**
     * Parses the specified query string and return the corresponding <code>Query</code> object.
     * 
     * @param query the <code>String</code> to parse
     * @return the <code>Query</code> object corresponding to the specified <code>String</code>.
     * @throws HorizonDBException 
     */
    public static Query parse(String query) throws HorizonDBException  {
              
        LOGGER.debug("parsing query: [{}]", query);
        
        final ErrorCollector errorCollector = new ErrorCollector();
        
        CharStream stream = new ANTLRInputStream(query);
        HqlLexer lexer = new HqlLexer(stream);
        lexer.removeErrorListeners();
        lexer.addErrorListener(errorCollector);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        
        if (errorCollector.hasError()) {
            
            ParsingError error = errorCollector.getError();
            
            throw new BadHqlGrammarException(query, 
                                             error.getLineNumber(), 
                                             error.getCharPosition(), 
                                             error.getMessage());
        }
        
        
        HqlParser parser = new HqlParser(tokens);
        parser.removeErrorListeners();
        parser.addErrorListener(errorCollector);

        StatementsContext statements = parser.statements();
        
        ParseTreeWalker walker = new ParseTreeWalker();
        QueryBuilder queryBuilder = new QueryBuilderDispatcher();
        walker.walk(queryBuilder, statements);
        
        if (errorCollector.hasError()) {
            
            ParsingError error = errorCollector.getError();
            
            throw new BadHqlGrammarException(query, 
                                             error.getLineNumber(), 
                                             error.getCharPosition(), 
                                             error.getMessage());
        }
        
        return queryBuilder.build();
    }
}
