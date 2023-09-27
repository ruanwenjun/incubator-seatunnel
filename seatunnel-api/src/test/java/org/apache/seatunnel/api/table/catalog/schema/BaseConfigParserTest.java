package org.apache.seatunnel.api.table.catalog.schema;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtilTest;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;

public class BaseConfigParserTest {

    protected Config getConfig(String configFile) throws FileNotFoundException, URISyntaxException {
        return ConfigFactory.parseFile(new File(getTestConfigFile(configFile)));
    }

    protected ReadonlyConfig getReadonlyConfig(String configFile)
            throws FileNotFoundException, URISyntaxException {
        return ReadonlyConfig.fromConfig(getConfig(configFile));
    }

    private String getTestConfigFile(String configFile)
            throws FileNotFoundException, URISyntaxException {
        URL resource = CatalogTableUtilTest.class.getResource(configFile);
        if (resource == null) {
            throw new FileNotFoundException("Can't find config file: " + configFile);
        }
        return Paths.get(resource.toURI()).toString();
    }
}
