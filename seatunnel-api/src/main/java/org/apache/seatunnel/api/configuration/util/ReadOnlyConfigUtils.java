package org.apache.seatunnel.api.configuration.util;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.experimental.UtilityClass;

@UtilityClass
public class ReadOnlyConfigUtils {

    public static <T> T getOrDefault(ReadonlyConfig config, Option<T> option, T defaultValue) {
        T value = config.get(option);
        return value == null ? defaultValue : value;
    }
}
