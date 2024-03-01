package com.se.flink.se.flink.handle.stateful.sink;

import com.se.flink.se.flink.handle.stateful.entity.Alert;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@PublicEvolving
@SuppressWarnings("unused")
public class AlertSink implements SinkFunction<Alert> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(AlertSink.class);

    @Override
    public void invoke(Alert value, Context context) {
        System.out.println(value.toString());
//        LOG.info(value.toString());
    }
}