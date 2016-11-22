package org.qcri.rheem.java.operators;

import org.apache.commons.io.IOUtils;
import org.qcri.rheem.basic.channels.FileChannel;
import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.UnarySource;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.core.util.fs.FileSystem;
import org.qcri.rheem.core.util.fs.FileSystems;
import org.qcri.rheem.java.channels.StreamChannel;
import org.qcri.rheem.java.execution.JavaExecutor;
import org.qcri.rheem.java.platform.JavaPlatform;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * {@link Operator} for the {@link JavaPlatform} that creates a sequence file. Consistent with Spark's object files.
 *
 * @see JavaObjectFileSink
 */
public class JavaTsvFileSource<T> extends UnarySource<T> implements JavaExecutionOperator {

    private final String sourcePath;

    public JavaTsvFileSource(String sourcePath, DataSetType type) {
        super(type);
        this.sourcePath = sourcePath;
    }

    public JavaTsvFileSource(DataSetType<Tuple2> type) {
        this(null, type);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            JavaExecutor javaExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        assert outputs.length == this.getNumOutputs();

        final String path;
        if (this.sourcePath == null) {
            final FileChannel.Instance input = (FileChannel.Instance) inputs[0];
            path = input.getSinglePath();
        } else {
            assert inputs.length == 0;
            path = this.sourcePath;
        }
        final String actualInputPath = FileSystems.findActualSingleInputPath(path);
        Stream<T> stream = this.createStream(actualInputPath);
        ((StreamChannel.Instance) outputs[0]).accept(stream);

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    private Stream<T> createStream(String path) {
        Function<String, T> parser = this::lineParse;

        return this.streamLines(path).map(parser);
    }

    private T lineParse(String line) {
        // TODO rewrite in less verbose way.
        Class typeClass = this.getType().getDataUnitType().getTypeClass();
        int tabPos = line.indexOf('\t');
        if (tabPos == -1) {
            if (typeClass == Integer.class) {
                return (T) Integer.valueOf(line);
            } else if (typeClass == Float.class) {
                return (T) Float.valueOf(line);
            } else if (typeClass == String.class) {
                return (T) String.valueOf(line);
            } else throw new RheemException(String.format("Cannot parse TSV file line %s", line));
        } else if (typeClass == Record.class) {
            // TODO: Fix Record parsing.
            return (T) new Record();
        } else if (typeClass == Tuple2.class) {
            // TODO: Fix Tuple2 parsing
            return (T) new Tuple2(
                    Integer.valueOf(line.substring(0, tabPos)),
                    Float.valueOf(line.substring(tabPos + 1)));
        } else
            throw new RheemException(String.format("Cannot parse TSV file line %s", line));

    }

    /**
     * Creates a {@link Stream} of a lines of the file.
     *
     * @param path of the file
     * @return the {@link Stream}
     */
    private Stream<String> streamLines(String path) {
        final FileSystem fileSystem = FileSystems.getFileSystem(path).orElseThrow(
                () -> new IllegalStateException(String.format("No file system found for %s", path))
        );
        try {
            Iterator<String> lineIterator = this.createLineIterator(fileSystem, path);
            return StreamSupport.stream(Spliterators.spliteratorUnknownSize(lineIterator, 0), false);
        } catch (IOException e) {
            throw new RheemException(String.format("%s failed to read %s.", this, path), e);
        }

    }

    /**
     * Creates an {@link Iterator} over the lines of a given {@code path} (that resides in the given {@code fileSystem}).
     */
    private Iterator<String> createLineIterator(FileSystem fileSystem, String path) throws IOException {
        final BufferedReader reader = new BufferedReader(new InputStreamReader(fileSystem.open(path), "UTF-8"));
        return new Iterator<String>() {

            String next;

            {
                this.advance();
            }

            private void advance() {
                try {
                    this.next = reader.readLine();
                } catch (IOException e) {
                    this.next = null;
                    throw new UncheckedIOException(e);
                } finally {
                    if (this.next == null) {
                        IOUtils.closeQuietly(reader);
                    }
                }
            }

            @Override
            public boolean hasNext() {
                return this.next != null;
            }

            @Override
            public String next() {
                assert this.hasNext();
                final String returnValue = this.next;
                this.advance();
                return returnValue;
            }
        };
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "rheem.java.tsvfilesource.load";
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new JavaTsvFileSource<>(this.sourcePath, this.getType());
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Collections.singletonList(FileChannel.HDFS_TSV_DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(StreamChannel.DESCRIPTOR);
    }

}
