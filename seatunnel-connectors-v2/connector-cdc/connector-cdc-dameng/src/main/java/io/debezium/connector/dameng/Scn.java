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

package io.debezium.connector.dameng;

import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Objects;

@EqualsAndHashCode
@RequiredArgsConstructor
public class Scn implements Comparable<Scn> {

    public static final Scn NULL = new Scn(null);
    public static final Scn MAX = new Scn(BigInteger.valueOf(-2));

    private final BigInteger scn;

    public boolean isNull() {
        return this.scn == null;
    }

    public static Scn valueOf(int value) {
        return new Scn(BigInteger.valueOf(value));
    }

    public static Scn valueOf(long value) {
        return new Scn(BigInteger.valueOf(value));
    }

    public static Scn valueOf(String value) {
        return new Scn(new BigInteger(value));
    }

    public static Scn valueOf(BigDecimal bigDecimal) {
        return new Scn(bigDecimal.unscaledValue());
    }

    public BigDecimal bigDecimalValue() {
        return isNull() ? null : new BigDecimal(scn);
    }

    public long longValue() {
        return isNull() ? 0 : scn.longValue();
    }

    public Scn add(Scn value) {
        if (isNull() && value.isNull()) {
            return Scn.NULL;
        } else if (value.isNull()) {
            return new Scn(scn);
        } else if (isNull()) {
            return new Scn(value.scn);
        }
        return new Scn(scn.add(value.scn));
    }

    public Scn subtract(Scn value) {
        if (isNull() && value.isNull()) {
            return Scn.NULL;
        } else if (value.isNull()) {
            return new Scn(scn);
        } else if (isNull()) {
            return new Scn(value.scn.negate());
        }
        return new Scn(scn.subtract(value.scn));
    }

    @Override
    public int compareTo(Scn o) {
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
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Scn scn1 = (Scn) o;
        return Objects.equals(scn, scn1.scn);
    }

    @Override
    public int hashCode() {
        return Objects.hash(scn);
    }

    @Override
    public String toString() {
        return isNull() ? "null" : scn.toString();
    }
}
