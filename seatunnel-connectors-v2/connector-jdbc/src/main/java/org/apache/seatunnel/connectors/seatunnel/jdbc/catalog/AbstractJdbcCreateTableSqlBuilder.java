package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog;

import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

// todo Abstract the concatenation common content of jdbc auto-build statements
public abstract class AbstractJdbcCreateTableSqlBuilder {

    protected boolean primaryCompareToConstrainKey(
            PrimaryKey primaryKey, ConstraintKey constraintKey) {
        List<String> columnNames = primaryKey.getColumnNames();
        List<ConstraintKey.ConstraintKeyColumn> constraintKeyColumnNames =
                constraintKey.getColumnNames();
        return columnNames.stream()
                .map(Object::toString)
                .collect(Collectors.toList())
                .containsAll(
                        constraintKeyColumnNames.stream()
                                .map(ConstraintKey.ConstraintKeyColumn::getColumnName)
                                .collect(Collectors.toList()));
    }

    public String getRandomStringSuffix() {
        return UUID.randomUUID().toString().replace("-", "").substring(0, 4);
    }
}
