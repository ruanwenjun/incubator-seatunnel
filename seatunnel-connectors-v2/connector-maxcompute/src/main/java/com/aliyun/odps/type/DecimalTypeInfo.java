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

package com.aliyun.odps.type;

import com.aliyun.odps.OdpsType;

/** We override this class to change the constructor to public. */
public class DecimalTypeInfo extends AbstractPrimitiveTypeInfo {
    static final int DEFAULT_PRECISION = 54;
    static final int DEFAULT_SCALE = 18;
    private int precision;
    private int scale;

    public DecimalTypeInfo() {
        this(54, 18);
    }

    public DecimalTypeInfo(int precision, int scale) {
        super(OdpsType.DECIMAL);
        this.validateParameter(precision, scale);
        this.precision = precision;
        this.scale = scale;
    }

    private void validateParameter(int precision, int scale) {
        if (precision < 1) {
            throw new IllegalArgumentException("Decimal precision < 1");
        } else if (scale < 0) {
            throw new IllegalArgumentException("Decimal scale < 0");
        } else if (scale > precision) {
            throw new IllegalArgumentException(
                    "Decimal precision must be larger than or equal to scale");
        }
    }

    public String getTypeName() {
        return this.precision == 54 && this.scale == 18
                ? super.getTypeName()
                : String.format("%s(%s,%s)", super.getTypeName(), this.precision, this.scale);
    }

    public int getPrecision() {
        return this.precision;
    }

    public int getScale() {
        return this.scale;
    }
}
