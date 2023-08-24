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

import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Date;

public class CompareUtils {
    // Compare two integers
    public static int compareIntegers(int num1, int num2) {
        return Integer.compare(num1, num2);
    }

    // Compare two long integers
    public static int compareLongs(long num1, long num2) {
        return Long.compare(num1, num2);
    }

    // Compare two floating-point numbers
    public static int compareFloats(float num1, float num2) {
        return Float.compare(num1, num2);
    }

    // Compare two double-precision floating-point numbers
    public static int compareDoubles(double num1, double num2) {
        return Double.compare(num1, num2);
    }

    // Compare two characters
    public static int compareCharacters(char char1, char char2) {
        return Character.compare(char1, char2);
    }

    // Compare two strings
    public static int compareStrings(String str1, String str2) {
        return str1.compareTo(str2);
    }

    // Compare two Dates
    public static int compareDates(Date date1, Date date2) {
        return date1.compareTo(date2);
    }

    // Compare two Times
    public static int compareTimes(Time time1, Time time2) {
        return time1.compareTo(time2);
    }

    // Compare two Timestamps
    public static int compareTimestamps(Timestamp timestamp1, Timestamp timestamp2) {
        return timestamp1.compareTo(timestamp2);
    }

    // Compare two BigDecimals
    public static int compareBigDecimals(BigDecimal num1, BigDecimal num2) {
        return num1.compareTo(num2);
    }

    public static int compareBooleans(boolean bool1, boolean bool2) {
        return Boolean.compare(bool1, bool2);
    }

    // Compare two short integers
    public static int compareShorts(short num1, short num2) {
        return Short.compare(num1, num2);
    }

    // Compare two LocalDate objects
    public static int compareLocalDates(LocalDate date1, LocalDate date2) {
        return date1.compareTo(date2);
    }

    // Compare two LocalDateTime objects
    public static int compareLocalDateTimes(LocalDateTime dateTime1, LocalDateTime dateTime2) {
        return dateTime1.compareTo(dateTime2);
    }

    // More comparison methods can be added based on the requirements
}
