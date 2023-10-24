package com.solace.connector.spark.receivers;

import com.solace.connector.spark.SolaceRecord;
import com.solacesystems.jcsmp.*;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class SolaceReliableReceiver extends Receiver<SolaceRecord> implements Serializable {

    private String hostName;
    private String vpnName;
    private String username;
    private String password;
    private String queue;
    private boolean isRunning;

    private static transient volatile JCSMPSession session;
    private static transient volatile FlowReceiver flowReceiver;
    private static transient volatile ConsumerFlowProperties flow_prop;

    private static final int BLOCKS_SIZE = 5;

    List<SolaceRecord> blocks = new ArrayList<>();

    private Thread receiver;

    public SolaceReliableReceiver(String host, String vpn, String username, String password, String queue) throws Exception {
        super(StorageLevel.MEMORY_AND_DISK());
        this.hostName = host;
        this.vpnName = vpn;
        this.username = username;
        this.password = password;
        this.queue = queue;

        initialize();
    }

    private void initialize() throws Exception {
        try {
//            log.info("SolaceSparkConnector - Initializing connection to Solace and establishing connection to " + this.queue + " Queue");
            JCSMPProperties jcsmpProperties = new JCSMPProperties();
            jcsmpProperties.setProperty(JCSMPProperties.HOST, this.hostName);
            jcsmpProperties.setProperty(JCSMPProperties.VPN_NAME, this.vpnName);
            jcsmpProperties.setProperty(JCSMPProperties.USERNAME, this.username);
            jcsmpProperties.setProperty(JCSMPProperties.PASSWORD, this.password);

            session = JCSMPFactory.onlyInstance().createSession(jcsmpProperties);
            session.connect();
//            log.info("SolaceSparkConnector - Connection Established to Solace");
            setUpQueue();
        } catch (InvalidPropertiesException e) {
//            log.error("SolaceSparkConnector - Invalid Solace Properties " + e.getMessage());
            close();
            throw new RuntimeException(e.getMessage());
        } catch (JCSMPException e) {
//            log.error("SolaceSparkConnector - Error connecting to Solace " + e.getMessage());
            close();
            throw new RuntimeException(e.getMessage());
        }
    }

    private void setUpQueue() throws Exception {
        try {
            Queue queue = JCSMPFactory.onlyInstance().createQueue(this.queue);
            flow_prop = new ConsumerFlowProperties();
            // Create a Flow be able to bind to and consume messages from the Queue.
            flow_prop.setEndpoint(queue);
            // will ack the messages in checkpoint
            flow_prop.setAckMode(JCSMPProperties.SUPPORTED_MESSAGE_ACK_CLIENT);
            flow_prop.setTransportWindowSize(255);
            flow_prop.setWindowedAckMaxSize(255);
            // bind to the queue, passing null as message listener for no async callback
            flowReceiver = session.createFlow(null, flow_prop, new EndpointProperties());
            flowReceiver.start();
//            log.info("SolaceSparkConnector - Connected to " + this.queue + " Queue");
        } catch (JCSMPException e) {
//            log.error("SolaceSparkConnector - Error connecting to Queue " + e.getMessage());
            close();
            throw new Exception(e.getMessage());
        }
    }

    private void close() {
        try {
//            this.isRunning = false;
            if (this.flowReceiver != null) {
//                log.info("SolaceSparkConnector - Closing flow receiver for Queue " + this.queue);
                this.flowReceiver.close();
            }

            if (this.session != null && !this.session.isClosed()) {
//                log.info("SolaceSparkConnector - Closing Solace session");
                this.session.closeSession();
            }
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public void onStart() {
        receiver = new Thread(() -> {
            while (!isStopped()) {
                try {
                    if(flowReceiver != null && !flowReceiver.isClosed()) {
                        BytesXMLMessage msg = flowReceiver.receive();
                        if (msg != null) {
                            if (blocks.size() == BLOCKS_SIZE) {
                                System.out.println("added@@");
                                store(blocks.iterator());
                                System.out.println("processed@@");
                                blocks.clear();
                            }
//                            reentrantLock.lock();
                            try {
                                Thread.sleep(50);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }

                            System.out.println("Received@@");
                            SolaceRecord solaceRecord = SolaceRecord.getMapper().map(msg);
                            blocks.add(solaceRecord);

                            if (blocks.size() == BLOCKS_SIZE) {
                                System.out.println("added@@");
                                store(blocks.iterator());
                                System.out.println("processed@@");
                                blocks.clear();
                            }
                        }
                    }
                } catch (Exception w) {
                    if(w instanceof NullPointerException) {
//                        log.error("SolaceSparkConnector - Exception while reading messages from Solace. Check if message id is null " + w);
                        throw new RuntimeException(w.getMessage());
                    } else {
//                        log.error("SolaceSparkConnector - Exception connecting to Solace " + w.toString());
                        try {
//                            log.info("SolaceSparkConnector - Retrying connection");
                            flowReceiver.close();
                            flowReceiver = session.createFlow(null, flow_prop, new EndpointProperties());
                            flowReceiver.start();
//                            log.info("SolaceSparkConnector - Reconnected successfully");
                        } catch (Exception restartEx) {
//                            log.error("SolaceSparkConnector - Error while reconnecting to Solace " + restartEx.getMessage());
                            close();
                            throw new RuntimeException(restartEx.getMessage());
                        }
                    }
                }
            }
        });
        receiver.start();
    }

    @Override
    public void onStop() {
//        receiver.interrupt();
    }
}
