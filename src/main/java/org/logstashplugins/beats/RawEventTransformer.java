package org.logstashplugins.beats;

import co.elastic.logstash.api.Codec;
import co.elastic.logstash.api.Event;

public class RawEventTransformer extends EventTransformer {

    public RawEventTransformer(Codec codec, boolean addHostname, boolean includeCodecTag) {
        super(codec, addHostname, includeCodecTag);
    }

    @Override
    public void transform(Event e) {
        super.transform(e);
        if (includeCodecTag) {
            e.tag("beats_input_raw_event");
        }

    }

    /*
    def transform(event)
      super(event)
      event.tag("beats_input_raw_event") if include_codec_tag?
      event
    end

     */
}
