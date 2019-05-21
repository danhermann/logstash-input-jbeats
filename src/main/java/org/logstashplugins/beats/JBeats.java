package org.logstashplugins.beats;

import co.elastic.logstash.api.Codec;
import co.elastic.logstash.api.Configuration;
import co.elastic.logstash.api.Context;
import co.elastic.logstash.api.Input;
import co.elastic.logstash.api.LogstashPlugin;
import co.elastic.logstash.api.PluginConfigSpec;
import co.elastic.logstash.api.PluginHelper;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

@LogstashPlugin(name = "jbeats")
public class JBeats implements Input {

    public static final PluginConfigSpec<Codec> CODEC_CONFIG =
            PluginConfigSpec.codecSetting("codec", "java_plain");

    public static final PluginConfigSpec<Long> PORT_CONFIG =
            PluginConfigSpec.numSetting("port", -1, false, true);

    public static final PluginConfigSpec<Long> EXECUTOR_THREADS_CONFIG =
            PluginConfigSpec.numSetting("executor_threads", Runtime.getRuntime().availableProcessors());

/*

  # The IP address to listen on.
  config :host, :validate => :string, :default => "0.0.0.0"

  # The port to listen on.
  config :port, :validate => :number, :required => true

  # JBeats handler executor thread
  config :executor_threads, :validate => :number, :default => LogStash::Config::CpuCoreStrategy.maximum

Runtime.getRuntime().availableProcessors();

*/

    private Codec codec;
    private final String id;
    private volatile boolean stopRequested = false;
    private final CountDownLatch countDownLatch;

    /**
     * Required constructor.
     *
     * @param id            Plugin id
     * @param configuration Logstash Configuration
     * @param context       Logstash Context
     */
    public JBeats(final String id, final Configuration configuration, final Context context) {
        this.id = id;
        this.countDownLatch = new CountDownLatch(1);
        this.codec = configuration.get(CODEC_CONFIG);
    }

    @Override
    public void start(Consumer<Map<String, Object>> consumer) {

    }

    @Override
    public void stop() {
        stopRequested = true;
    }

    @Override
    public void awaitStop() throws InterruptedException {
        countDownLatch.await();
    }

    @Override
    public Collection<PluginConfigSpec<?>> configSchema() {
        return PluginHelper.commonInputSettings(Arrays.asList(EXECUTOR_THREADS_CONFIG, PORT_CONFIG));
    }

    @Override
    public String getId() {
        return id;
    }
}
