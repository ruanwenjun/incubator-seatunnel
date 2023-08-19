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

package org.apache.seatunnel.connectors.cdc.informix.source.offset;

import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;

import io.debezium.connector.informix.Lsn;
import io.debezium.connector.informix.SourceInfo;

import java.util.HashMap;
import java.util.Map;

public class InformixOffset extends Offset {
    public static final InformixOffset INITIAL_OFFSET = new InformixOffset(Lsn.valueOf(0));
    public static final InformixOffset NO_STOPPING_OFFSET =
            new InformixOffset(Lsn.valueOf(Long.MAX_VALUE));

    public InformixOffset(Lsn LSN) {
        this(LSN, null);
    }

    public InformixOffset(Lsn changeLsn, Lsn commitLsn) {
        this(createOffsetMap(changeLsn, commitLsn));
    }

    public InformixOffset(Map<String, String> offsetMap) {
        this.offset = offsetMap;
    }

    public Lsn getScn() {
        return Lsn.valueOf(offset.get(SourceInfo.CHANGE_LSN_KEY));
    }

    public Lsn getCommitScn() {
        return Lsn.valueOf(offset.get(SourceInfo.COMMIT_LSN_KEY));
    }

    @Override
    public int compareTo(Offset o) {
        InformixOffset that = (InformixOffset) o;
        if (NO_STOPPING_OFFSET.equals(that) && NO_STOPPING_OFFSET.equals(this)) {
            return 0;
        }
        if (NO_STOPPING_OFFSET.equals(this)) {
            return 1;
        }
        if (NO_STOPPING_OFFSET.equals(that)) {
            return -1;
        }

        Lsn thisChangeLsn = this.getScn();
        Lsn thatChangeLsn = that.getScn();
        return thisChangeLsn.compareTo(thatChangeLsn);
    }

    @SuppressWarnings("checkstyle:EqualsHashCode")
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof InformixOffset)) {
            return false;
        }
        InformixOffset that = (InformixOffset) o;
        return offset.equals(that.offset);
    }

    private static Map<String, String> createOffsetMap(Lsn changeLsn, Lsn commitLsn) {
        Map<String, String> offsetMap = new HashMap<>();
        if (changeLsn != null) {
            offsetMap.put(SourceInfo.CHANGE_LSN_KEY, changeLsn.toString());
        }
        if (commitLsn != null) {
            offsetMap.put(SourceInfo.COMMIT_LSN_KEY, commitLsn.toString());
        }
        return offsetMap;
    }
}
