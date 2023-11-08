package com.solace.connector.spark.streaming.solace;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.solace.connector.spark.SolaceRecord;
import com.solace.connector.spark.streaming.solace.utils.SolaceUtils;
import com.solacesystems.jcsmp.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.App;


public class EventListener implements XMLMessageListener, Serializable {

    private static Logger log = LoggerFactory.getLogger(EventListener.class);
    private AppSingleton appSingleton;

    public void setAppSingleton(AppSingleton appSingleton) {
        this.appSingleton = appSingleton;
    }
    @Override
    public void onReceive(BytesXMLMessage msg) {
        try {

//            System.out.println("Message received. ......");
//            log.info("SolaceSparkConnector - Message received from Solace");
//            SolaceRecord solaceRecord = SolaceRecord.getMapper().map(msg);
            String messageID = SolaceUtils.getMessageID(msg, this.appSingleton.solaceOffsetIndicator);
            this.appSingleton.messageMap.put(messageID, new SolaceMessage(msg));
//            this.appSingleton.messages.add(solaceRecord);

//            log.info("SolaceSparkConnector - Message added to internal map. Count :: " + this.appSingleton.messages.size());

//            log.info(AppSingleton.getInstance().debits.toString());

//            System.out.println("====+++++====");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public void onException(JCSMPException e) {
        System.out.printf("Consumer received exception: %s%n", e);
    }



}
