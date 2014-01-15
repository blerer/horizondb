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

import io.horizondb.io.ByteReader;
import io.horizondb.io.encoding.VarInts;
import io.horizondb.io.files.SeekableFileDataInput;

import java.io.IOException;

import static io.horizondb.io.encoding.VarInts.readInt;

/**
 * @author Benjamin
 * 
 */
public class IntegerAndStringNodeReader extends AbstractNodeReader<Integer, String> {

    public static final NodeReaderFactory<Integer, String> FACTORY = new NodeReaderFactory<Integer, String>() {

        @Override
        public NodeReader<Integer, String> newReader(SeekableFileDataInput input) throws IOException {
            return new IntegerAndStringNodeReader(input);
        }

    };

    /**
     * @param input
     * @throws IOException
     */
    public IntegerAndStringNodeReader(SeekableFileDataInput input) throws IOException {
        super(input);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Integer readKey(ByteReader reader) throws IOException {
        return Integer.valueOf(readInt(reader));
    }

    /**
     * {@inheritDoc}
     * 
     * @throws IOException
     */
    @Override
    protected String readValue(ByteReader reader) throws IOException {
        return VarInts.readString(reader);
    }
}
