package com.solacecoe.connectors.spark.streaming.solace.utils;

import com.solacecoe.connectors.spark.streaming.solace.SolaceBroker;
import com.solacesystems.jcsmp.SessionEvent;
import com.solacesystems.jcsmp.SessionEventArgs;
import com.solacesystems.jcsmp.SessionEventHandler;

public class SolaceSessionEventListener implements SessionEventHandler {
    private final SolaceBroker solaceBroker;
    public SolaceSessionEventListener(SolaceBroker solaceBroker) {
        this.solaceBroker = solaceBroker;
    }
    @Override
    public void handleEvent(SessionEventArgs sessionEventArgs) {
        if(sessionEventArgs != null) {
            if(sessionEventArgs.getEvent().compareTo(SessionEvent.DOWN_ERROR) > -1) {
                this.solaceBroker.close();
            }
        }
    }
}
