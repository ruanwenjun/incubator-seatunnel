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

package org.apache.seatunnel.connectors.seatunnel.cdc.oracleAgent.source.offset;

import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;

import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** A structure describes an offset in oracle 9 bridge event. */
public class OracleAgentOffset extends Offset {

    private static final long serialVersionUID = 1L;

    // we use fzs_file_number and scn to represent the consumed offset
    public static final String FZS_FILE_NUMBER_KEY = "fzs_file_number";
    public static final String SCN_KEY = "scn";

    public static final OracleAgentOffset INITIAL_OFFSET = new OracleAgentOffset(0, 0L);
    public static final OracleAgentOffset NO_STOPPING_OFFSET =
            new OracleAgentOffset(Integer.MAX_VALUE, Long.MAX_VALUE);

    public OracleAgentOffset(Map<String, String> offset) {
        this.offset = offset;
    }

    public OracleAgentOffset(Integer fzsFileNumber, Long scn) {
        Map<String, String> offsetMap = new HashMap<>();
        if (fzsFileNumber != null) {
            offsetMap.put(FZS_FILE_NUMBER_KEY, String.valueOf(fzsFileNumber));
        }
        if (scn != null) {
            offsetMap.put(SCN_KEY, String.valueOf(scn));
        }
        this.offset = offsetMap;
    }

    public Optional<Long> getScn() {
        String scn = offset.get(SCN_KEY);
        if (StringUtils.isEmpty(scn)) {
            return Optional.empty();
        }
        return Optional.of(Long.parseLong(scn));
    }

    public Optional<Integer> getFzsFileNumber() {
        String fzsFileNumber = offset.get(FZS_FILE_NUMBER_KEY);
        if (StringUtils.isEmpty(fzsFileNumber)) {
            return Optional.empty();
        }
        return Optional.of(Integer.valueOf(offset.get(FZS_FILE_NUMBER_KEY)));
    }

    @Override
    public int compareTo(Offset offset) {
        OracleAgentOffset that = (OracleAgentOffset) offset;
        // the NO_STOPPING_OFFSET is the max offset
        if (NO_STOPPING_OFFSET.equals(that) && NO_STOPPING_OFFSET.equals(this)) {
            return 0;
        }
        if (NO_STOPPING_OFFSET.equals(this)) {
            return 1;
        }
        if (NO_STOPPING_OFFSET.equals(that)) {
            return -1;
        }

        Optional<Long> thisScn = getScn();
        Optional<Long> thatScn = that.getScn();
        if (thisScn.isPresent() && thatScn.isPresent()) {
            return thisScn.get().compareTo(thatScn.get());
        }
        if (thisScn.isPresent()) {
            return 1;
        }
        if (thatScn.isPresent()) {
            return -1;
        }
        return 0;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        Optional<Long> scn = getScn();
        if (scn.isPresent()) {
            result = prime * result + scn.hashCode();
        }
        Optional<Integer> fzsFileNumber = getFzsFileNumber();
        if (fzsFileNumber.isPresent()) {
            result = prime * result + fzsFileNumber.hashCode();
        }
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof OracleAgentOffset)) {
            return false;
        }
        OracleAgentOffset that = (OracleAgentOffset) o;
        return offset.equals(that.offset);
    }
}
