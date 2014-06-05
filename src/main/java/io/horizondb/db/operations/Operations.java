/**
 * Copyright 2014 Benjamin Lerer
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
package io.horizondb.db.operations;

import io.horizondb.db.Operation;
import io.horizondb.model.protocol.OpCode;

import java.util.EnumMap;
import java.util.Map;

/**
 * The mapping between operation code and the actual operations.
 * 
 * @author Benjamin
 *
 */
public final class Operations {

    /**
     * The Operations instance.
     */
    private static final Operations INSTANCE = new Operations();
    
    /**
     * The mapping between the operation code and the action to execute.
     */
    private final Map<OpCode, Operation> operations;
    
    /**
     * Returns the <code>Operation</code> associated to the specified operation code.
     * 
     * @param opCode the operation code 
     * @return the <code>Operation</code> associated to the specified code or <code>null</code>
     * if the code is unknown
     */
    public static Operation getOperationFor(OpCode opCode) {
        
        return INSTANCE.operations.get(opCode);
    }
        
    /**
     * Creates a new <code>Operations</code> instance.
     */
    private Operations() {
        
        this.operations = new EnumMap<>(OpCode.class);
        this.operations.put(OpCode.CREATE_DATABASE, new CreateDatabaseOperation());
        this.operations.put(OpCode.USE_DATABASE, new UseDatabaseOperation());
        this.operations.put(OpCode.CREATE_TIMESERIES, new CreateTimeSeriesOperation());
        this.operations.put(OpCode.SELECT, new SelectOperation());
        this.operations.put(OpCode.INSERT, new InsertOperation());
    }
}
