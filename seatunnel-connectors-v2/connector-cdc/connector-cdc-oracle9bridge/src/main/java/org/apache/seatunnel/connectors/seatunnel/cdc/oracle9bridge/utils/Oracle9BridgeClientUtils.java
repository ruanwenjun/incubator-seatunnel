package org.apache.seatunnel.connectors.seatunnel.cdc.oracle9bridge.utils;

import org.apache.seatunnel.common.utils.RetryUtils;

import org.apache.commons.collections4.CollectionUtils;

import org.whaleops.whaletunnel.oracle9bridge.sdk.Oracle9BridgeClient;
import org.whaleops.whaletunnel.oracle9bridge.sdk.Oracle9BridgeException;
import org.whaleops.whaletunnel.oracle9bridge.sdk.model.OracleOperation;
import org.whaleops.whaletunnel.oracle9bridge.sdk.model.OracleTransactionData;
import org.whaleops.whaletunnel.oracle9bridge.sdk.model.OracleTransactionDataFetchRequest;
import org.whaleops.whaletunnel.oracle9bridge.sdk.model.OracleTransactionFileNumberFetchRequest;

import lombok.extern.slf4j.Slf4j;

import java.math.BigInteger;
import java.util.Collections;
import java.util.List;

@Slf4j
public class Oracle9BridgeClientUtils {

    public static List<OracleTransactionData> fetchOracleTransactionData(
            Oracle9BridgeClient oracle9BridgeClient,
            List<String> tableOwners,
            List<String> tables,
            Integer fzsFileNumber) {
        return fetchOracleTransactionData(
                oracle9BridgeClient,
                new OracleTransactionDataFetchRequest(tableOwners, tables, fzsFileNumber));
    }

    public static List<OracleTransactionData> fetchOracleTransactionData(
            Oracle9BridgeClient oracle9BridgeClient, OracleTransactionDataFetchRequest request) {
        try {
            return oracle9BridgeClient.fetchTransactionData(request);
        } catch (Oracle9BridgeException e) {
            throw new RuntimeException("Query oracle transaction error, request: " + request, e);
        }
    }

    public static Long currentMaxScn(
            Oracle9BridgeClient oracle9BridgeClient,
            String tableOwner,
            String table,
            Integer fzsFileNumber) {
        return currentMaxScn(
                oracle9BridgeClient,
                Collections.singletonList(tableOwner),
                Collections.singletonList(table),
                fzsFileNumber);
    }

    public static Long currentMaxScn(
            Oracle9BridgeClient oracle9BridgeClient,
            List<String> tableOwners,
            List<String> tables,
            Integer fzsFileNumber) {
        List<OracleTransactionData> oracleTransactionData =
                fetchOracleTransactionData(
                        oracle9BridgeClient,
                        new OracleTransactionDataFetchRequest(tableOwners, tables, fzsFileNumber));
        if (oracleTransactionData.isEmpty()) {
            return null;
        }
        long maxScn = 0L;
        for (OracleTransactionData transactionData : oracleTransactionData) {
            for (OracleOperation op : transactionData.getOp()) {
                String scn = op.getScn();
                BigInteger bigInteger = new BigInteger(scn, 16);
                maxScn = Math.max(maxScn, bigInteger.longValue());
            }
        }
        return maxScn;
    }

    public static Long currentMinScn(
            Oracle9BridgeClient oracle9BridgeClient,
            String tableOwner,
            String table,
            Integer fzsFileNumber) {
        return currentMinScn(
                oracle9BridgeClient,
                Collections.singletonList(tableOwner),
                Collections.singletonList(table),
                fzsFileNumber);
    }

    public static Long currentMinScn(
            Oracle9BridgeClient oracle9BridgeClient,
            List<String> tableOwners,
            List<String> tables,
            Integer fzsFileNumber) {
        List<OracleTransactionData> oracleTransactionData =
                fetchOracleTransactionData(
                        oracle9BridgeClient,
                        new OracleTransactionDataFetchRequest(tableOwners, tables, fzsFileNumber));
        if (oracleTransactionData.isEmpty()) {
            return null;
        }
        long minScn = 0L;
        for (OracleTransactionData transactionData : oracleTransactionData) {
            // todo: directly return the first scn?
            for (OracleOperation op : transactionData.getOp()) {
                String scn = op.getScn();
                BigInteger bigInteger = new BigInteger(scn, 16);
                minScn = Math.min(minScn, bigInteger.longValue());
            }
        }
        return minScn;
    }

    public static Integer currentMinFzsFileNumber(
            Oracle9BridgeClient oracle9BridgeClient, String tableOwner, String table) {
        return currentMaxFzsFileNumber(
                oracle9BridgeClient,
                new OracleTransactionFileNumberFetchRequest(tableOwner, table));
    }

    public static Integer currentMinFzsFileNumber(
            Oracle9BridgeClient oracle9BridgeClient, List<String> tableOwner, List<String> table) {
        return currentMinFzsFileNumber(
                oracle9BridgeClient,
                new OracleTransactionFileNumberFetchRequest(tableOwner, table));
    }

    public static Integer currentMinFzsFileNumber(
            Oracle9BridgeClient oracle9BridgeClient,
            OracleTransactionFileNumberFetchRequest fetchRequest) {
        // todo: can we directly get the min fzs number?
        Integer maxTransactionFileNumber =
                currentMaxFzsFileNumber(oracle9BridgeClient, fetchRequest);
        if (maxTransactionFileNumber == null) {
            throw new RuntimeException(
                    "Query min file number error, request: "
                            + fetchRequest
                            + " , Please check if the fzs file is missing");
        }
        int minFzsFileNumber = 0;
        while (minFzsFileNumber < maxTransactionFileNumber) {
            int mid = (minFzsFileNumber + maxTransactionFileNumber) / 2;
            List<OracleTransactionData> oracleTransactionData =
                    fetchOracleTransactionData(
                            oracle9BridgeClient,
                            new OracleTransactionDataFetchRequest(
                                    fetchRequest.getTableOwner(), fetchRequest.getTable(), mid));
            if (CollectionUtils.isNotEmpty(oracleTransactionData)) {
                maxTransactionFileNumber = mid;
            } else {
                minFzsFileNumber = mid + 1;
            }
        }
        log.info("Current min fzs file number: {}", minFzsFileNumber);
        minFzsFileNumber = Math.max(minFzsFileNumber, 0);
        return minFzsFileNumber;
    }

    public static Integer currentMaxFzsFileNumber(
            Oracle9BridgeClient oracle9BridgeClient, String tableOwner, String table) {
        return currentMaxFzsFileNumber(
                oracle9BridgeClient,
                new OracleTransactionFileNumberFetchRequest(tableOwner, table));
    }

    public static Integer currentMaxFzsFileNumber(
            Oracle9BridgeClient oracle9BridgeClient, List<String> tableOwner, List<String> table) {
        return currentMaxFzsFileNumber(
                oracle9BridgeClient,
                new OracleTransactionFileNumberFetchRequest(tableOwner, table));
    }

    public static Integer currentMaxFzsFileNumber(
            Oracle9BridgeClient oracle9BridgeClient,
            OracleTransactionFileNumberFetchRequest fetchRequest) {
        // get the max fzs and scn
        try {
            Integer maxFzsFileNumber =
                    RetryUtils.retryWithException(
                            () ->
                                    oracle9BridgeClient.getCurrentMaxTransactionFileNumber(
                                            fetchRequest),
                            new RetryUtils.RetryMaterial(3, true, e -> true, 1_000));
            log.info("Current max fzs file number: {}", maxFzsFileNumber);
            // avoid -1
            maxFzsFileNumber = Math.max(maxFzsFileNumber, 0);
            return maxFzsFileNumber;
        } catch (Exception e) {
            throw new RuntimeException(
                    "Query max file number error, fetchRequest: " + fetchRequest, e);
        }
    }
}
