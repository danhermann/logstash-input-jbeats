package org.logstashplugins.beats;

import co.elastic.logstash.api.Codec;
import co.elastic.logstash.api.Event;
import co.elastic.logstash.api.EventFactory;
import co.elastic.logstash.api.NamespacedMetric;
import io.netty.channel.ChannelHandlerContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;


public class MessageListener implements IMessageListener {

    private static final String FILEBEAT_LOG_LINE_FIELD = "message";
    private static final String LSF_LOG_LINE_FIELD = "line";
    private static final String CURRENT_CONNECTIONS = "current_connections";
    private static final String PEAK_CONNECTIONS = "peak_connections";
    private static final Logger logger = LogManager.getLogger(MessageListener.class);

    private final EventFactory eventFactory;
    private final Consumer<Map<String, Object>> consumer;
    private final ConcurrentHashMap<ChannelHandlerContext, ConnectionState> connections;
    private final NamespacedMetric metric;
    private final Codec codec;
    private final DecodedEventTransformer decodedEventTransformer;
    private final RawEventTransformer rawEventTransformer;

    private int peakConnectionCount;

    public MessageListener() {
        throw new UnsupportedOperationException();
    }

    public MessageListener(
            Consumer<Map<String, Object>> consumer,
            EventFactory eventFactory,
            NamespacedMetric metric,
            Codec codec,
            boolean addHostname,
            boolean includeCodecTag) {
        this.consumer = consumer;
        this.peakConnectionCount = 0;
        this.connections = new ConcurrentHashMap<>();
        this.metric = metric;
        this.codec = codec;
        this.eventFactory = eventFactory;
        this.decodedEventTransformer = new DecodedEventTransformer(codec, addHostname, includeCodecTag);
        this.rawEventTransformer = new RawEventTransformer(codec, addHostname, includeCodecTag);
    }

    /**
     * This is triggered on every new message parsed by the beats handler.
     */
    public void onNewMessage(ChannelHandlerContext ctx, Message message) {
        Map hash = message.getData();
        String ipAddress = getIpAddressForContext(ctx);
        if (ipAddress != null && hash.get("@metadata") != null) {
            // set ipAddress
        }

        String targetField = extractTargetField(hash);

        // extractTlsPeer

        if (targetField == null) {
            Event e = eventFactory.newEvent(hash);
            rawEventTransformer.transform(e);
            consumer.accept(e.getData());
        } else {

        }


/*

      hash = message.getData
      ip_address = ip_address(ctx)

      hash['@metadata']['ip_address'] = ip_address unless ip_address.nil? || hash['@metadata'].nil?
      target_field = extract_target_field(hash)

      extract_tls_peer(hash, ctx)

      if target_field.nil?
        event = LogStash::Event.new(hash)
        @nocodec_transformer.transform(event)
        @queue << event
      else
        codec(ctx).accept(CodecCallbackListener.new(target_field,
                                                    hash,
                                                    message.getIdentityStream(),
                                                    @codec_transformer,
                                                    @queue))
      end

 */
    }

    /**
     * Triggered when a new client connects to the input and used to link a connection
     * to a codec.
     */
    public void onNewConnection(ChannelHandlerContext ctx) {
        registerConnection(ctx);
        incrementConnectionCount();
    }

    /**
     * Triggered when a connection is closed on the remote end and we need to flush buffered
     * events to the queue.
     */
    public void onConnectionClose(ChannelHandlerContext ctx) {
        unregisterConnection(ctx);
        decrementConnectionCount();
    }

    /**
     * Called when something bad occurs in the pipeline. Allow us to clear the buffered codec
     * when an error occurs.
     */
    public void onException(ChannelHandlerContext ctx, Throwable cause) {
        unregisterConnection(ctx);
    }

    /**
     * Called when a error occur in channel initialization, usually an SSL handshake error.
     */
    public void onChannelInitializeException(ChannelHandlerContext ctx, Throwable cause) {
        if (cause instanceof IllegalArgumentException) {
            logger.error("Error likely caused by a bad certificate, an invalid key, or a private key not in PKCS8 format.", cause);
        } else {
            logger.warn("Error creating a connection", cause);
        }
    }

    private void unregisterConnection(ChannelHandlerContext context) {
        if (context != null && connections.get(context) != null) {
            flushBuffer(context);
            connections.remove(context);
        }
    }

    private void flushBuffer(ChannelHandlerContext context) {
        Codec codec = connections.get(context).codec;
        if (codec != null) {
            EventTransformer eventTransformer = new EventTransformer(codec, false, false);
            eventTransformer.transform(null);
            consumer.accept(null);
        }
        throw new UnsupportedOperationException();
    }

    private String extractTargetField(Map data) {
        if (isFilebeat(data)) {
            return data.get(FILEBEAT_LOG_LINE_FIELD).toString();
        } else if (isLogstashForwarder(data)) {
            return data.get(LSF_LOG_LINE_FIELD).toString();
        } else {
            throw new IllegalArgumentException("Unknown source type in Beats input");
        }
    }

    private boolean isFilebeat(Map data) {
        return data.get(FILEBEAT_LOG_LINE_FIELD) != null;
    }

    private boolean isLogstashForwarder(Map data) {
        return data.get(LSF_LOG_LINE_FIELD) != null;
    }

    private void registerConnection(ChannelHandlerContext context) {
        connections.put(context, new ConnectionState(context, codec.cloneCodec(), extractIpAddress(context)));
    }

    private String extractIpAddress(ChannelHandlerContext context) {
        SocketAddress sa = context.channel().remoteAddress();
        if (sa instanceof InetSocketAddress) {
            return ((InetSocketAddress) sa).getAddress().getHostAddress();
        }
        logger.debug("Could not retrieve remote IP address for beats input because remoteAddress is null");
        return null;
    }

    private Codec getCodecForContext(ChannelHandlerContext context) {
        ConnectionState cs;
        if ((cs = connections.get(context)) != null) {
            return cs.codec;
        }
        return null;
    }

    private String getIpAddressForContext(ChannelHandlerContext context) {
        ConnectionState cs;
        if ((cs = connections.get(context)) != null) {
            return cs.ipAddress;
        }
        return null;
    }

    private synchronized void incrementConnectionCount() {
        int currentConnectionCount = connections.size();
        metric.gauge(CURRENT_CONNECTIONS, currentConnectionCount);
        if (currentConnectionCount > peakConnectionCount) {
            peakConnectionCount = currentConnectionCount;
            metric.gauge(PEAK_CONNECTIONS, peakConnectionCount);
        }
    }

    private void decrementConnectionCount() {
        metric.gauge(CURRENT_CONNECTIONS, connections.size());
    }
}

class ConnectionState {
    public ChannelHandlerContext context;
    public Codec codec;
    public String ipAddress;

    public ConnectionState(ChannelHandlerContext context, Codec codec, String ipAddress) {
        this.context = context;
        this.codec = codec;
        this.ipAddress = ipAddress;
    }
}
