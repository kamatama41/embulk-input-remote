package org.embulk.test;

import com.google.common.collect.ImmutableList;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.Exec;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;
import org.embulk.spi.util.Pages;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class MemoryOutputPlugin implements OutputPlugin
{
    public interface PluginTask extends Task { }

    @Override
    public ConfigDiff transaction(ConfigSource config,
                                  Schema schema, int taskCount,
                                  OutputPlugin.Control control)
    {
        final PluginTask task = config.loadConfig(PluginTask.class);
        return resume(task.dump(), schema, taskCount, control);
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
                             Schema schema, int taskCount,
                             OutputPlugin.Control control)
    {
        control.run(taskSource);
        return Exec.newConfigDiff();
    }

    @Override
    public void cleanup(TaskSource taskSource,
                        Schema schema, int taskCount,
                        List<TaskReport> successTaskReports)
    { }

    @Override
    public TransactionalPageOutput open(final TaskSource taskSource, final Schema schema, final int taskIndex)
    {
        return new TransactionalPageOutput()
        {
            private final PageReader reader = new PageReader(schema);

            public void add(Page page)
            {
                reader.setPage(page);
                while (reader.nextRecord())
                {
                    Recorder.addRecord(reader);
                }
            }

            public void finish() { }

            public void close()
            {
                reader.close();
            }

            public void abort() { }

            public TaskReport commit()
            {
                return Exec.newTaskReport();
            }
        };
    }

    public static void clearRecords()
    {
        Recorder.clearRecords();
    }

    public static List<Record> getRecords()
    {
        return Recorder.getRecords();
    }

    private static class Recorder
    {
        private static List<Record> records = new ArrayList<>();

        private Recorder() { }

        private synchronized static void addRecord(PageReader reader)
        {
            final ImmutableList.Builder<Object> values = ImmutableList.builder();
            final ImmutableList.Builder<Column> columns = ImmutableList.builder();
            reader.getSchema().visitColumns(new Pages.ObjectColumnVisitor(reader) {
                @Override
                public void visit(org.embulk.spi.Column column, Object value) {
                    values.add(value);
                    columns.add(column);
                }
            });
            records.add(new Record(values.build(), columns.build()));
        }

        static void clearRecords()
        {
            records = new ArrayList<>();
        }

        static List<Record> getRecords()
        {
            return new ArrayList<>(records);
        }
    }

    public static class Record
    {
        private final List<Object> values;
        private final List<Column> columns;

        Record(List<Object> values, List<Column> columns)
        {
            this.values = values;
            this.columns = columns;
        }

        public List<Object> getValues()
        {
            return values;
        }

        public List<Column> getColumns()
        {
            return columns;
        }
    }

    public static void assertValues(List... valuesList) {
        Set<List> actual = new HashSet<>();
        for (Record record : getRecords()) {
            actual.add(record.getValues());
        }

        Set<List> expected = new HashSet<>();
        Collections.addAll(expected, valuesList);

        assertThat(actual, is(expected));
    }

    public static List values(Object... values) {
        return Arrays.asList(values);
    }
}
