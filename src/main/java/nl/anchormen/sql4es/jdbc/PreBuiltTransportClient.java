package nl.anchormen.sql4es.jdbc;

import io.netty.util.ThreadDeathWatcher;
import org.elasticsearch.client.transport.TransportClient;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.painless.PainlessPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.Netty4Plugin;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class PreBuiltTransportClient extends TransportClient {
    private static final Collection<Class<? extends Plugin>> PRE_INSTALLED_PLUGINS;

    private static void initializeNetty() {
        setSystemPropertyIfUnset("io.netty.noUnsafe", Boolean.toString(true));
        setSystemPropertyIfUnset("io.netty.noKeySetOptimization", Boolean.toString(true));
        setSystemPropertyIfUnset("io.netty.recycler.maxCapacityPerThread", Integer.toString(0));
    }

    @SuppressForbidden(
            reason = "set system properties to configure Netty"
    )
    private static void setSystemPropertyIfUnset(String key, String value) {
        String currentValue = System.getProperty(key);
        if (currentValue == null) {
            System.setProperty(key, value);
        }

    }

    @SafeVarargs
    public PreBuiltTransportClient(Settings settings, Class... plugins) {
        this(settings, (Collection)Arrays.asList(plugins));
    }

    public PreBuiltTransportClient(Settings settings, Collection<Class<? extends Plugin>> plugins) {
        this(settings, plugins, (TransportClient.HostFailureListener)null);
    }

    public PreBuiltTransportClient(Settings settings, Collection<Class<? extends Plugin>> plugins, TransportClient.HostFailureListener hostFailureListener) {
        super(settings, Settings.EMPTY, addPlugins(plugins, PRE_INSTALLED_PLUGINS), hostFailureListener);
    }

    public void close() {
        super.close();
        if (!NetworkModule.TRANSPORT_TYPE_SETTING.exists(this.settings) || ((String)NetworkModule.TRANSPORT_TYPE_SETTING.get(this.settings)).equals("netty4")) {
            try {
                GlobalEventExecutor.INSTANCE.awaitInactivity(5L, TimeUnit.SECONDS);
            } catch (InterruptedException var3) {
                Thread.currentThread().interrupt();
            }

            try {
                ThreadDeathWatcher.awaitInactivity(5L, TimeUnit.SECONDS);
            } catch (InterruptedException var2) {
                Thread.currentThread().interrupt();
            }
        }

    }

    static {
        initializeNetty();
        PRE_INSTALLED_PLUGINS = Collections.unmodifiableList(Arrays.asList(Netty4Plugin.class, PainlessPlugin.class));
    }
}
