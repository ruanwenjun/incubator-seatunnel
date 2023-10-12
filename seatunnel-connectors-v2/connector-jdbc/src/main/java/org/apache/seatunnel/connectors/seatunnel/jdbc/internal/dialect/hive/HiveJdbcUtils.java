package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.hive;

import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcConnectionConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

import static org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorErrorCode.KERBEROS_AUTHENTICATION_FAILED;

@Slf4j
public class HiveJdbcUtils {

    public static synchronized void doKerberosAuthentication(JdbcConnectionConfig jdbcConfig) {
        String principal = jdbcConfig.kerberosPrincipal;
        String keytabPath = jdbcConfig.kerberosKeytabPath;
        String krb5Path = jdbcConfig.krb5Path;
        System.setProperty("java.security.krb5.conf", krb5Path);
        Configuration configuration = new Configuration();

        if (StringUtils.isBlank(principal) || StringUtils.isBlank(keytabPath)) {
            log.warn(
                    "Principal [{}] or keytabPath [{}] is empty, it will skip kerberos authentication",
                    principal,
                    keytabPath);
        } else {
            configuration.set("hadoop.security.authentication", "kerberos");
            UserGroupInformation.setConfiguration(configuration);
            try {
                log.info(
                        "Start Kerberos authentication using principal {} and keytab {}",
                        principal,
                        keytabPath);
                UserGroupInformation.loginUserFromKeytab(principal, keytabPath);
                log.info("Kerberos authentication successful");
            } catch (IOException e) {
                String errorMsg =
                        String.format(
                                "Kerberos authentication failed using this "
                                        + "principal [%s] and keytab path [%s]",
                                principal, keytabPath);
                throw new JdbcConnectorException(KERBEROS_AUTHENTICATION_FAILED, errorMsg, e);
            }
        }
    }
}
