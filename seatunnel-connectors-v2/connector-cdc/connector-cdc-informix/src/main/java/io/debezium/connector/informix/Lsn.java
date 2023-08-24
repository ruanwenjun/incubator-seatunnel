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

package io.debezium.connector.informix;

import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;

import java.math.BigDecimal;
import java.math.BigInteger;

@EqualsAndHashCode
@RequiredArgsConstructor
public class Lsn implements Comparable<Lsn> {
    private final BigInteger scn;

    public boolean isNull() {
        return this.scn == null;
    }

    public static Lsn valueOf(int value) {
        return new Lsn(BigInteger.valueOf(value));
    }

    public static Lsn valueOf(long value) {
        return new Lsn(BigInteger.valueOf(value));
    }

    public static Lsn valueOf(String value) {
        return new Lsn(new BigInteger(value));
    }

    public static Lsn valueOf(BigDecimal bigDecimal) {
        return new Lsn(bigDecimal.unscaledValue());
    }

    public BigDecimal bigDecimalValue() {
        return isNull() ? null : new BigDecimal(scn);
    }

    public long longValue() {
        return isNull() ? 0L : scn.longValue();
    }

    @Override
    public int compareTo(Lsn o) {
        if (isNull() && o.isNull()) {
            return 0;
        } else if (isNull() && !o.isNull()) {
            return -1;
        } else if (!isNull() && o.isNull()) {
            return 1;
        }
        return scn.compareTo(o.scn);
    }

    @Override
    public String toString() {
        return scn.toString();
    }
}
