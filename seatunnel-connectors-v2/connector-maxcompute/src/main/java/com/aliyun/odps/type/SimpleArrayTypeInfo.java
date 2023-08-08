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

import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.maxcompute.exception.MaxcomputeConnectorException;

import com.aliyun.odps.OdpsType;

public class SimpleArrayTypeInfo implements ArrayTypeInfo {
    private final TypeInfo valueType;

    public SimpleArrayTypeInfo(TypeInfo typeInfo) {
        if (typeInfo == null) {
            throw new MaxcomputeConnectorException(
                    CommonErrorCode.UNSUPPORTED_DATA_TYPE, "Invalid element type.");
        } else {
            this.valueType = typeInfo;
        }
    }

    public String getTypeName() {
        return this.getOdpsType().name() + "<" + this.valueType.getTypeName() + ">";
    }

    public TypeInfo getElementTypeInfo() {
        return this.valueType;
    }

    public OdpsType getOdpsType() {
        return OdpsType.ARRAY;
    }

    public String toString() {
        return this.getTypeName();
    }
}
