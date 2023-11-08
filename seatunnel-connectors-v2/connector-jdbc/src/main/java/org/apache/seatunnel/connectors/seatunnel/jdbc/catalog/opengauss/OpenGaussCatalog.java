package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.opengauss;

import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.psql.PostgresCatalog;

public class OpenGaussCatalog extends PostgresCatalog {

    static {
        SYS_DATABASES.clear();
        SYS_DATABASES.add("omm");
        SYS_DATABASES.add("template0");
        SYS_DATABASES.add("template1");
    }

    public OpenGaussCatalog(
            String catalogName,
            String username,
            String pwd,
            JdbcUrlUtil.UrlInfo urlInfo,
            String defaultSchema) {
        super(catalogName, username, pwd, urlInfo, defaultSchema);
    }
}
