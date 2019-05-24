package org.logstashplugins.beats;

import co.elastic.logstash.api.Codec;
import co.elastic.logstash.api.Event;

public class DecodedEventTransformer extends EventTransformer {

    public DecodedEventTransformer(Codec codec, boolean addHostname, boolean includeCodecTag) {
        super(codec, addHostname, includeCodecTag);
    }

    @Override
    public void transform(Event e) {
        super.transform(e);
    }

/*
    def transform(event, hash)
      ts = coerce_ts(hash.delete("@timestamp"))

      event.set("@timestamp", ts) unless ts.nil?
      hash.each { |k, v| event.set(k, v) }
      super(event)
      event.tag("beats_input_codec_#{codec_name}_applied") if include_codec_tag?
      event
    end

    private
    def coerce_ts(ts)
      return nil if ts.nil?
      timestamp = LogStash::Timestamp.coerce(ts)

      return timestamp if timestamp

      @logger.warn("Unrecognized @timestamp value, setting current time to @timestamp",
                   :value => ts.inspect)
      return nil
    rescue LogStash::TimestampParserError => e
      @logger.warn("Error parsing @timestamp string, setting current time to @timestamp",
                   :value => ts.inspect, :exception => e.message)
      return nil
    end
  end


 */
}
