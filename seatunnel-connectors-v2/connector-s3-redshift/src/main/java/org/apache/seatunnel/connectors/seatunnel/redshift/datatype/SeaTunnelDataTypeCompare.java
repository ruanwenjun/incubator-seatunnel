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

package org.apache.seatunnel.connectors.seatunnel.redshift.datatype;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.connectors.seatunnel.redshift.exception.S3RedshiftConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.redshift.exception.S3RedshiftConnectorException;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

public class SeaTunnelDataTypeCompare {

    public static int compare(
            String fieldName, SeaTunnelDataType dataType, Object val1, Object val2) {
        if (val1 == null || val2 == null) {
            throw new S3RedshiftConnectorException(
                    S3RedshiftConnectorErrorCode.COMPARE_FIELD_IS_NULL,
                    String.format(
                            "field name:%s, field type:%s", fieldName, dataType.getTypeClass()));
        }
        if (dataType instanceof DecimalType) {
            return CompareUtils.compareBigDecimals((BigDecimal) val1, (BigDecimal) val2);
        } else if (dataType.equals(BasicType.BOOLEAN_TYPE)) {
            return CompareUtils.compareBooleans((Boolean) val1, (Boolean) val2);
        } else if (dataType.equals(BasicType.INT_TYPE)) {
            return CompareUtils.compareIntegers((Integer) val1, (Integer) val2);
        } else if (dataType.equals(BasicType.LONG_TYPE)) {
            return CompareUtils.compareLongs((Long) val1, (Long) val2);
        } else if (dataType.equals(BasicType.SHORT_TYPE)) {
            return CompareUtils.compareShorts((Short) val1, (Short) val2);
        } else if (dataType.equals(LocalTimeType.LOCAL_DATE_TYPE)) {
            return CompareUtils.compareLocalDates((LocalDate) val1, (LocalDate) val2);
        } else if (dataType.equals(LocalTimeType.LOCAL_DATE_TIME_TYPE)) {
            return CompareUtils.compareLocalDateTimes((LocalDateTime) val1, (LocalDateTime) val2);
        } else if (dataType.equals(BasicType.STRING_TYPE)) {
            return CompareUtils.compareStrings(val1.toString(), val2.toString());
        } else if (dataType.equals(BasicType.FLOAT_TYPE)) {
            return CompareUtils.compareFloats((Float) val1, (Float) val2);
        } else if (dataType.equals(BasicType.DOUBLE_TYPE)) {
            return CompareUtils.compareDoubles((Double) val1, (Double) val2);
        } else {
            throw new S3RedshiftConnectorException(
                    S3RedshiftConnectorErrorCode.UNSUPPORTED_COMPARE_FIELD,
                    String.format(
                            "field name:%s, field type:%s", fieldName, dataType.getTypeClass()));
        }
    }
}
