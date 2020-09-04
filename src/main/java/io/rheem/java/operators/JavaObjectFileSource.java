package io.rheem.java.operators;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import io.rheem.basic.channels.FileChannel;
import io.rheem.core.api.exception.RheemException;
import io.rheem.core.optimizer.OptimizationContext;
import io.rheem.core.plan.rheemplan.ExecutionOperator;
import io.rheem.core.plan.rheemplan.Operator;
import io.rheem.core.plan.rheemplan.UnarySource;
import io.rheem.core.platform.ChannelDescriptor;
import io.rheem.core.platform.ChannelInstance;
import io.rheem.core.platform.lineage.ExecutionLineageNode;
import io.rheem.core.types.DataSetType;
import io.rheem.core.util.Tuple;
import io.rheem.core.util.fs.FileSystems;
import io.rheem.java.channels.StreamChannel;
import io.rheem.java.execution.JavaExecutor;
import io.rheem.java.platform.JavaPlatform;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * {@link Operator} for the {@link JavaPlatform} that creates a sequence file. Consistent with Spark's object files.
 *
 * @see JavaObjectFileSink
 */
public class JavaObjectFileSource<T> extends UnarySource<T> implements JavaExecutionOperator {

    private final String sourcePath;

    public JavaObjectFileSource(DataSetType<T> type) {
        this(null, type);
    }

    public JavaObjectFileSource(String sourcePath, DataSetType<T> type) {
        super(type);
        this.sourcePath = sourcePath;
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            JavaExecutor javaExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        assert outputs.length == this.getNumOutputs();

        SequenceFileIterator sequenceFileIterator;
        final String path;
        if (this.sourcePath == null) {
            final FileChannel.Instance input = (FileChannel.Instance) inputs[0];
            path = input.getSinglePath();
        } else {
            assert inputs.length == 0;
            path = this.sourcePath;
        }
        try {
            final String actualInputPath = FileSystems.findActualSingleInputPath(path);
            sequenceFileIterator = new SequenceFileIterator<>(actualInputPath);
            Stream<?> sequenceFileStream =
                    StreamSupport.stream(Spliterators.spliteratorUnknownSize(sequenceFileIterator, 0), false);
            ((StreamChannel.Instance) outputs[0]).accept(sequenceFileStream);
        } catch (IOException e) {
            throw new RheemException(String.format("%s failed to read from %s.", this, path), e);
        }

        return ExecutionOperator.modelEagerExecution(inputs, outputs, operatorContext);
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "rheem.java.objectfilesource.load";
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new JavaObjectFileSource<>(this.sourcePath, this.getType());
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        assert index <= this.getNumInputs() || (index == 0 && this.getNumInputs() == 0);
        return Collections.singletonList(FileChannel.HDFS_OBJECT_FILE_DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(StreamChannel.DESCRIPTOR);
    }

    private static class SequenceFileIterator<T> implements Iterator<T>, AutoCloseable, Closeable {

        private SequenceFile.Reader sequenceFileReader;

        private final NullWritable nullWritable = NullWritable.get();

        private final BytesWritable bytesWritable = new BytesWritable();

        private Object[] nextElements;

        private ArrayList nextElements_cole;

        private int nextIndex;

        SequenceFileIterator(String path) throws IOException {
            final SequenceFile.Reader.Option fileOption = SequenceFile.Reader.file(new Path(path));
            this.sequenceFileReader = new SequenceFile.Reader(new Configuration(true), fileOption);
            Validate.isTrue(this.sequenceFileReader.getKeyClass().equals(NullWritable.class));
            Validate.isTrue(this.sequenceFileReader.getValueClass().equals(BytesWritable.class));
            this.tryAdvance();
        }

        private void tryAdvance() {
            if (this.nextElements != null && ++this.nextIndex < this.nextElements.length) return;
            if (this.nextElements_cole != null && ++this.nextIndex < this.nextElements_cole.size()) return;
            try {
                if (!this.sequenceFileReader.next(this.nullWritable, this.bytesWritable)) {
                    this.nextElements = null;
                    return;
                }
                Object tmp = new ObjectInputStream(new ByteArrayInputStream(this.bytesWritable.getBytes())).readObject();
                if(tmp instanceof Collection) {
                    this.nextElements = null;
                    this.nextElements_cole = (ArrayList) tmp;
                }else if(tmp instanceof Object[]){
                    this.nextElements = (Object[]) tmp;
                    this.nextElements_cole = null;
                }else {
                    this.nextElements = new Object[1];
                    this.nextElements[0] = tmp;

                }
                this.nextIndex = 0;
            } catch (IOException | ClassNotFoundException e) {
                this.nextElements = null;
                IOUtils.closeQuietly(this);
                throw new RheemException("Reading failed.", e);
            }
        }

        @Override
        public boolean hasNext() {
            return this.nextElements != null || this.nextElements_cole != null;
        }

        @Override
        public T next() {
            Validate.isTrue(this.hasNext());
            @SuppressWarnings("unchecked")
            final T result;
            if(this.nextElements_cole != null){
                result = (T) this.nextElements_cole.get(this.nextIndex);
            }else if (this.nextElements != null) {
                result = (T) this.nextElements[this.nextIndex];
            }else{
                result = null;
            }

            this.tryAdvance();
            return result;
        }

        @Override
        public void close() {
            if (this.sequenceFileReader != null) {
                try {
                    this.sequenceFileReader.close();
                } catch (Throwable t) {
                    LoggerFactory.getLogger(this.getClass()).error("Closing failed.", t);
                }
                this.sequenceFileReader = null;
            }
        }
    }

}
