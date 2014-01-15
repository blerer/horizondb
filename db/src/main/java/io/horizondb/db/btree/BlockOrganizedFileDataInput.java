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

import io.horizondb.io.ReadableBuffer;
import io.horizondb.io.files.SeekableFileDataInput;

import java.io.IOException;

import static org.apache.commons.lang.Validate.isTrue;
import static org.apache.commons.lang.Validate.notNull;

/**
 * @author Benjamin
 * 
 */
class BlockOrganizedFileDataInput extends AbstractBlockOrganizedByteReader implements SeekableFileDataInput {

    private final SeekableFileDataInput input;

    /**
     * @param input
     * @throws IOException
     */
    public BlockOrganizedFileDataInput(int blockSize, SeekableFileDataInput input) throws IOException {

        super(blockSize);

        notNull(input, "the input parameter must not be null.");

        this.input = input;

        if (!isFileEmpty()) {
            handleNewBlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void seek(long position) throws IOException {

        isTrue(position < size(), "Position greater than the current size" + "(pos: " + position + " size: " + size()
                + ").");

        long numberOfBlocks = position / (getBlockSize() - 1);
        long blockPosition = numberOfBlocks * getBlockSize();

        this.input.seek(blockPosition);
        handleNewBlock();

        long realPosition = position + numberOfBlocks + 1;

        this.input.seek(realPosition);
    }

    public boolean seekHeader() throws IOException {

        int numberOfBlocks = getNumberOfCompletedBlocks();

        if (numberOfBlocks == 0) {

            return false;
        }

        do {

            seekBlock(--numberOfBlocks);

        } while (!isHeaderBlock() && numberOfBlocks > 0);

        return isHeaderBlock();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException {
        this.input.close();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long readableBytes() throws IOException {
        return size() - getPosition();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long size() throws IOException {

        long realSize = getRealSize();
        long numberOfBlocks = getNumberOfBlocks(realSize);

        return realSize - numberOfBlocks;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getPosition() throws IOException {

        long realPosition = this.input.getPosition();

        long numberOfBlocks = getNumberOfBlocks(realPosition);

        return realPosition - numberOfBlocks;
    }

    /**
     * Returns <code>true</code> if the file is empty, <code>false</code> otherwise.
     * 
     * @return <code>true</code> if the file is empty, <code>false</code> otherwise.
     * @throws IOException if an I/O problem occurs.
     */
    public boolean isFileEmpty() throws IOException {
        return this.input.size() == 0;
    }

    /**
     * Returns the number of blocks used for the specified position.
     * 
     * @param position the position
     * @return the number of blocks used for the specified position.
     */
    private long getNumberOfBlocks(long position) {

        return (long) Math.ceil(((double) position) / getBlockSize());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected long getRealPosition() throws IOException {
        return this.input.getPosition();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected byte doReadByte() throws IOException {
        return this.input.readByte();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void doReadBytes(byte[] bytes, int offset, int length) throws IOException {

        this.input.readBytes(bytes, offset, length);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected ReadableBuffer doSlice(int length) throws IOException {

        return this.input.slice(length);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected long getRealSize() throws IOException {
        return this.input.size();
    }

    /**
     * @param blockNumber
     * @throws IOException
     */
    private void seekBlock(long blockNumber) throws IOException {

        long blockPosition = (blockNumber * getBlockSize());
        this.input.seek(blockPosition);

        handleNewBlock();
    }

    /**
     * Returns the number of completed blocks.
     * 
     * @return the number of completed blocks.
     * @throws IOException if an I/O problem occurs while retrieving the file real size.
     */
    private int getNumberOfCompletedBlocks() throws IOException {

        return (int) (getRealSize() / getBlockSize());
    }
}
