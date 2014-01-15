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
package io.horizondb.db.btree;

import io.horizondb.db.btree.AbstractNodeWriter;
import io.horizondb.db.btree.NodeWriter;
import io.horizondb.db.btree.NodeWriterFactory;
import io.horizondb.io.ByteWriter;
import io.horizondb.io.files.FileDataOutput;

import java.io.IOException;

import static io.horizondb.io.encoding.VarInts.computeIntSize;
import static io.horizondb.io.encoding.VarInts.computeStringSize;
import static io.horizondb.io.encoding.VarInts.writeInt;
import static io.horizondb.io.encoding.VarInts.writeString;

/**
 * @author Benjamin
 * 
 */
public class IntegerAndStringNodeWriter extends AbstractNodeWriter<Integer, String> {

    public static final NodeWriterFactory<Integer, String> FACTORY = new NodeWriterFactory<Integer, String>() {

        @Override
        public NodeWriter<Integer, String> newWriter(FileDataOutput output) throws IOException {
            return new IntegerAndStringNodeWriter(output);
        }
    };

    public IntegerAndStringNodeWriter(FileDataOutput output) throws IOException {

        super(output);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void writeValue(ByteWriter writer, String value) throws IOException {
        writeString(writer, value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected int computeKeySize(Integer key) {

        return computeIntSize(key.intValue());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void writeKey(ByteWriter writer, Integer key) throws IOException {

        writeInt(writer, key.intValue());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected int computeValueSize(String value) {

        return computeStringSize(value);
    }
}
