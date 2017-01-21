package org.embulk.test;

import org.embulk.EmbulkEmbed;
import org.embulk.config.ConfigSource;

import static com.google.common.base.Strings.isNullOrEmpty;
import static org.embulk.test.EmbulkTests.readResource;
import static org.hamcrest.Matchers.is;
import static org.junit.Assume.assumeThat;

public class MyEmbulkTests {
    private MyEmbulkTests() {
    }

    public static ConfigSource configFromString(String yaml) {
        assumeThat(isNullOrEmpty(yaml), is(false));
        return EmbulkEmbed.newSystemConfigLoader().fromYamlString(yaml);
    }

    public static ConfigSource configFromResource(String name) {
        return configFromString(readResource(name));
    }
}
