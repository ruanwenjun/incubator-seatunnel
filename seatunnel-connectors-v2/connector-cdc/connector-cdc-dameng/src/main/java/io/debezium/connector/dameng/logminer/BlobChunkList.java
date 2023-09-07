/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.debezium.connector.dameng.logminer;

import io.debezium.connector.dameng.DamengValueConverters;

import java.util.ArrayList;
import java.util.List;

/**
 * A "marker" class for passing a collection of Blob data type chunks to {@link
 * DamengValueConverters} so that each chunk can be converted, decoded, and combined into a single
 * binary representation for event emission.
 *
 * @author Chris Cranford
 */
public class BlobChunkList extends ArrayList<String> {
    /**
     * Creates a BLOB chunk list backed by the provided collection.
     *
     * @param backingList collection of BLOB chunks
     */
    public BlobChunkList(List<String> backingList) {
        super(backingList);
    }
}
