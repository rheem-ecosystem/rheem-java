package io.rheem.java.operators;

import org.junit.Assert;
import org.junit.Test;
import io.rheem.core.api.Configuration;
import io.rheem.core.api.Job;
import io.rheem.core.function.TransformationDescriptor;
import io.rheem.core.optimizer.OptimizationContext;
import io.rheem.core.plan.rheemplan.OutputSlot;
import io.rheem.core.platform.ChannelInstance;
import io.rheem.core.util.fs.LocalFileSystem;
import io.rheem.java.channels.StreamChannel;
import io.rheem.java.execution.JavaExecutor;
import io.rheem.java.platform.JavaPlatform;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test suite for {@link JavaTextFileSink}.
 */
public class JavaTextFileSinkTest extends JavaExecutionOperatorTestBase {

    @Test
    public void testWritingLocalFile() throws IOException, URISyntaxException {
        Configuration configuration = new Configuration();

        final File tempDir = LocalFileSystem.findTempDir();
        final String targetUrl = LocalFileSystem.toURL(new File(tempDir, "testWritingLocalFile.txt"));
        JavaTextFileSink<Float> sink = new JavaTextFileSink<>(
                targetUrl,
                new TransformationDescriptor<>(
                        f -> String.format("%.2f", f),
                        Float.class, String.class
                )
        );

        Job job = mock(Job.class);
        when(job.getConfiguration()).thenReturn(configuration);
        final JavaExecutor javaExecutor = (JavaExecutor) JavaPlatform.getInstance().createExecutor(job);

        StreamChannel.Instance inputChannelInstance = (StreamChannel.Instance) StreamChannel.DESCRIPTOR
                .createChannel(mock(OutputSlot.class), configuration)
                .createInstance(javaExecutor, mock(OptimizationContext.OperatorContext.class), 0);
        inputChannelInstance.accept(Stream.of(1.123f, -0.1f, 3f));
        evaluate(sink, new ChannelInstance[]{inputChannelInstance}, new ChannelInstance[0]);


        final List<String> lines = Files.lines(Paths.get(new URI(targetUrl))).collect(Collectors.toList());
        Assert.assertEquals(
                Arrays.asList("1.12", "-0.10", "3.00"),
                lines
        );

    }

}
