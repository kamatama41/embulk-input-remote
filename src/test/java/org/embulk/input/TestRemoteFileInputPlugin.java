package org.embulk.input;

import org.embulk.config.ConfigSource;
import org.embulk.spi.InputPlugin;
import org.embulk.test.EmbulkTests;
import org.embulk.test.TestingEmbulk;
import org.junit.Rule;
import org.junit.Test;

import java.nio.file.Path;

import static org.embulk.test.EmbulkTests.readResource;
import static org.embulk.test.EmbulkTests.readSortedFile;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class TestRemoteFileInputPlugin
{
    @Rule
    public TestingEmbulk embulk = TestingEmbulk
            .builder()
            .registerPlugin(InputPlugin.class, "remote", RemoteFileInputPlugin.class)
            .build();

    @Test
    public void loadFromRemote() throws Exception
    {
        ConfigSource config = EmbulkTests.config("YAML_TEST01");
        Path out = embulk.createTempFile("csv");

        embulk.runInput(config, out);

        assertThat(
                readSortedFile(out),
                is(readResource("expect/test01.csv")));
    }

}
