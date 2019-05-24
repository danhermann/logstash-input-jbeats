package org.logstashplugins.beats;

import co.elastic.logstash.api.Codec;
import co.elastic.logstash.api.Event;

public class EventTransformer {

    private static final String BEATS_HOSTNAME_FIELD = "[beat][hostname]";
    private static final String HOST_FIELD = "host";

    private final Codec codec;
    protected final boolean addHostname;
    protected final boolean includeCodecTag;

    public EventTransformer(Codec codec, boolean addHostname, boolean includeCodecTag) {
        this.codec = codec;
        this.addHostname = addHostname;
        this.includeCodecTag = includeCodecTag;
    }

    public void copyBeatHostname(Event e) {
        if (addHostname) {
            Object host = e.getField(BEATS_HOSTNAME_FIELD);
            if (host != null && e.getField(HOST_FIELD) == null) {
                e.setField(HOST_FIELD, host);
            }
        }

    }

    public String codecName() {
        return codec != null ? codec.getName() : "<none>";
    }

    public void transform(Event e) {
        copyBeatHostname(e);
    }


/*


    def codec_name
      @codec_name ||= if @input.codec.respond_to?(:base_codec)
                        @input.codec.base_codec.class.config_name
                      else
                        @input.codec.class.config_name
                      end
    end

    def transform(event)
      copy_beat_hostname(event)
      decorate(event)
      event
    end

    def include_codec_tag?
      @input.include_codec_tag
    end

 */
}
