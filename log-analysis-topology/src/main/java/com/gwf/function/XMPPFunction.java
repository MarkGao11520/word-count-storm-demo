package com.gwf.function;

import com.gwf.common.MessageMapper;
import org.jivesoftware.smack.ConnectionConfiguration;
import org.jivesoftware.smack.XMPPConnection;
import org.jivesoftware.smack.XMPPException;
import org.jivesoftware.smack.packet.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.Map;

public class XMPPFunction extends BaseFunction{
    private static final long serialVersionUID = -4606192636575490395L;
    private static final Logger LOG = LoggerFactory.getLogger(XMPPFunction.class);

    public static final String XMPP_TO = "storm.xmpp.to";
    public static final String XMPP_USER = "storm.xmpp.user";
    public static final String XMPP_PASSWORD = "storm.xmpp.password";
    public static final String XMPP_SERVER = "storm.xmpp.server";

    private XMPPConnection xmppConnection;
    private String to;
    private MessageMapper mapper;

    public XMPPFunction(MessageMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        LOG.debug("Prepare: {}",conf);
        super.prepare(conf,context);
        this.to = (String)conf.get(XMPP_TO);
        ConnectionConfiguration configuration = new ConnectionConfiguration((String) conf.get(XMPP_SERVER));
        this.xmppConnection = new XMPPConnection(configuration);
        try {
            this.xmppConnection.connect();
            this.xmppConnection.login((String) conf.get(XMPP_USER),(String)conf.get(XMPP_PASSWORD));
        } catch (XMPPException e) {
            LOG.warn("Error initialzing XMPP Channel", e);
        }
    }

    @Override
    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
        Message message = new Message(this.to, Message.Type.normal);
        message.setBody(this.mapper.toMessageBody(tridentTuple));
        this.xmppConnection.sendPacket(message);
    }
}
