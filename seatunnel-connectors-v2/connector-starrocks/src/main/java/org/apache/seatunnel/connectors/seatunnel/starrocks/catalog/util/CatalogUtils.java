package org.apache.seatunnel.connectors.seatunnel.starrocks.catalog.util;

import org.apache.seatunnel.connectors.seatunnel.starrocks.dialectenum.FieldIdeEnum;

public class CatalogUtils {
    public static String getFieldIde(String identifier, String fieldIde) {
        if (fieldIde == null) {
            return identifier;
        }
        switch (FieldIdeEnum.valueOf(fieldIde.toUpperCase())) {
            case LOWERCASE:
                return identifier.toLowerCase();
            case UPPERCASE:
                return identifier.toUpperCase();
            default:
                return identifier;
        }
    }

    public static String quoteIdentifier(String identifier, String fieldIde, String quote) {
        if (identifier.contains(".")) {
            String[] parts = identifier.split("\\.");
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < parts.length - 1; i++) {
                sb.append(quote).append(parts[i]).append(quote).append(".");
            }
            return sb.append(quote)
                    .append(getFieldIde(parts[parts.length - 1], fieldIde))
                    .append(quote)
                    .toString();
        }

        return quote + getFieldIde(identifier, fieldIde) + quote;
    }

    public static String quoteIdentifier(String identifier, String fieldIde) {
        return getFieldIde(identifier, fieldIde);
    }

    public static String quoteTableIdentifier(String identifier, String fieldIde) {
        if (identifier.contains(".")) {
            String[] parts = identifier.split("\\.");
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < parts.length - 1; i++) {
                sb.append(parts[i]).append(".");
            }
            return sb.append(getFieldIde(parts[parts.length - 1], fieldIde)).toString();
        }

        return getFieldIde(identifier, fieldIde);
    }
}
