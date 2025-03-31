package com.solacecoe.connectors.spark.base;

import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;

public class SolaceSession {
    private JCSMPSession session;
    private final String host;
    private final String vpn;
    private final String username;
    private final String password;

    public SolaceSession(String host, String vpn, String username, String password) {
        this.host = host;
        this.vpn = vpn;
        this.username = username;
        this.password = password;

        try {
            final JCSMPProperties properties = new JCSMPProperties();
            properties.setProperty(JCSMPProperties.HOST, this.host);            // host:port
            properties.setProperty(JCSMPProperties.USERNAME, this.username); // client-username
            properties.setProperty(JCSMPProperties.VPN_NAME, this.vpn);    // message-vpn
            properties.setProperty(JCSMPProperties.PASSWORD, this.password); // client-password
            session = JCSMPFactory.onlyInstance().createSession(properties);
            session.connect();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public JCSMPSession getSession() {
        return session;
    }
}
