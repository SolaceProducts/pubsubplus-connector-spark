package com.solacecoe.connectors.spark.streaming.solace;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class AppSingleton implements Serializable {
    private static Logger log = LoggerFactory.getLogger(AppSingleton.class);
    static private AppSingleton instance;
    private EventListener listener = null;

//    public ConcurrentLinkedQueue<SolaceRecord> messages;

    public ConcurrentHashMap<String, SolaceMessage> messageMap;

    public List<String> processedMessageIDs;

    public String solaceOffsetIndicator = "MESSAGE_ID";


    private AppSingleton() {
//        this.messages = new ConcurrentLinkedQueue<>();
        this.messageMap = new ConcurrentHashMap();
        this.processedMessageIDs = new ArrayList<>();
//        log.info("AppSingleton CREATED!!!");

    }
    public static AppSingleton getInstance() {
        if (instance == null) {
            log.info("SolaceSparkConnector - AppSingleton Instance is null");
            instance = new AppSingleton();
        }
        return instance;
    }

    public void setCallback(EventListener listener){
        this.listener = listener;

    }
    public EventListener getCallback(){
        return this.listener;
    }

}
