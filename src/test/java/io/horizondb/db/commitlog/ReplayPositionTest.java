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
package io.horizondb.db.commitlog;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author Benjamin
 *
 */
public class ReplayPositionTest {

    @Test
    public void testIsAfter() {
        
        ReplayPosition pos = new ReplayPosition(0, 1);
        ReplayPosition pos2 = new ReplayPosition(0, 2);
        
        assertFalse(pos.isAfter(pos2));
        assertTrue(pos2.isAfter(pos));
        
        pos2 = new ReplayPosition(1, 1);
        
        assertFalse(pos.isAfter(pos2));
        assertTrue(pos2.isAfter(pos));
        
        pos2 = new ReplayPosition(0, 1);
        
        assertFalse(pos.isAfter(pos2));
        assertFalse(pos2.isAfter(pos));
    }

}
