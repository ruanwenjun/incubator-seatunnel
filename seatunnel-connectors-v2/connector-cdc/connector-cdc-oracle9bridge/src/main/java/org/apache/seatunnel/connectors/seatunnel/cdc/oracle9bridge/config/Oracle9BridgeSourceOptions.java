package org.apache.seatunnel.connectors.seatunnel.cdc.oracle9bridge.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.SingleChoiceOption;
import org.apache.seatunnel.connectors.cdc.base.option.SourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.base.option.StopMode;

import java.util.Arrays;

public class Oracle9BridgeSourceOptions {

    public static final Option<String> ORACLE9BRIDGE_AGENT_HOST =
            Options.key("oracle9bridge-agent-host")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The host of the Oracle agent. e.g. localhost");

    public static final Option<Integer> ORACLE9BRIDGE_AGENT_PORT =
            Options.key("oracle9bridge-agent-port")
                    .intType()
                    .noDefaultValue()
                    .withDescription("The port of the Oracle agent. e.g. 8190");

    public static final SingleChoiceOption<StartupMode> STARTUP_MODE =
            (SingleChoiceOption<StartupMode>)
                    Options.key(SourceOptions.STARTUP_MODE_KEY)
                            .singleChoice(
                                    StartupMode.class,
                                    Arrays.asList(
                                            StartupMode.INITIAL,
                                            StartupMode.EARLIEST,
                                            StartupMode.LATEST))
                            .defaultValue(StartupMode.INITIAL)
                            .withDescription(
                                    "Optional startup mode for CDC source, valid enumerations are "
                                            + "\"initial\", \"earliest\", \"latest\"");

    public static final SingleChoiceOption<StopMode> STOP_MODE =
            (SingleChoiceOption<StopMode>)
                    Options.key(SourceOptions.STOP_MODE_KEY)
                            .singleChoice(StopMode.class, Arrays.asList(StopMode.NEVER))
                            .defaultValue(StopMode.NEVER)
                            .withDescription("Optional stop mode for CDC source");
}
