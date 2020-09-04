package io.rheem.java.plugin;

import io.rheem.core.api.Configuration;
import io.rheem.core.mapping.Mapping;
import io.rheem.core.optimizer.channels.ChannelConversion;
import io.rheem.core.platform.Platform;
import io.rheem.core.plugin.Plugin;
import io.rheem.java.channels.ChannelConversions;
import io.rheem.java.platform.JavaPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * This {@link Plugin} is a subset of the {@link JavaBasicPlugin} and only ships with the {@link ChannelConversion}s.
 */
public class JavaChannelConversionPlugin implements Plugin {

    @Override
    public Collection<Mapping> getMappings() {
        return Collections.emptyList();
    }

    @Override
    public Collection<ChannelConversion> getChannelConversions() {
        return ChannelConversions.ALL;
    }

    @Override
    public Collection<Platform> getRequiredPlatforms() {
        return Collections.singleton(JavaPlatform.getInstance());
    }

    @Override
    public void setProperties(Configuration configuration) {
        // Nothing to do, because we already configured the properties in #configureDefaults(...).
    }

}
