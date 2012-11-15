package com.psddev.dari.util;

import java.util.Map;

import java.util.Properties;

import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Provides STMP mail support **/
public class SmtpMailProvider extends AbstractMailProvider {
    
    private final static Logger logger = 
        LoggerFactory.getLogger(SmtpMailProvider.class);
    
    private String host;
    private String username;
    private String password;
    
    private boolean useTLS;
    private long tlsPort = 587;
    
    private boolean useSSL;
    private long sslPort = 465;
    
    public SmtpMailProvider() {
    }
    
    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public boolean isUseTLS() {
        return useTLS;
    }

    public void setUseTLS(boolean useTLS) {
        this.useTLS = useTLS;
    }

    public long getTlsPort() {
        return tlsPort;
    }

    public void setTlsPort(long tlsPort) {
        this.tlsPort = tlsPort;
    }

    public boolean isUseSSL() {
        return useSSL;
    }

    public void setUseSSL(boolean useSSL) {
        this.useSSL = useSSL;
    }

    public long getSslPort() {
        return sslPort;
    }

    public void setSslPort(long sslPort) {
        this.sslPort = sslPort;
    }
    
    // --- MailProvider support ---

    @Override
    public void sendMail(MailMessage emailMessage) {
        if (StringUtils.isEmpty(host)) {
            String errorText = "SMTP Host can't be null!";
            logger.error(errorText);
            throw new IllegalArgumentException(errorText);
        }
        if (emailMessage == null) {
            String errorText = "EmailMessage can't be null!";
            logger.error(errorText);
            throw new IllegalArgumentException(errorText);
        }
                  
        Session session;

        Properties props = new Properties();
        props.put("mail.transport.protocol", "smtp");
        props.put("mail.smtp.host", host);

        if (useTLS) {
            props.put("mail.smtp.starttls.enable", "true");
            props.put("mail.smtp.port", tlsPort);
        } else if (useSSL) { /** Note - not really tested **/
            props.put("mail.smtp.socketFactory.port", sslPort);
            props.put("mail.smtp.socketFactory.class", 
                "javax.net.ssl.SSLSocketFactory");
            props.put("mail.smtp.port", sslPort);
        }

        if (!StringUtils.isEmpty(username) && 
                !StringUtils.isEmpty(password)) {
            props.put("mail.smtp.auth", "true");
            session = Session.getInstance(props, 
                new javax.mail.Authenticator() {
                    @Override
                    protected PasswordAuthentication getPasswordAuthentication() {
                        return new PasswordAuthentication(username, password);
                    }
                });
        } else {
            session = Session.getInstance(props);
        }

        try {
            Message message = new MimeMessage(session);

            // From
            message.setFrom(new InternetAddress(emailMessage
                .getFromAddress()));
            // To
            message.setRecipients(Message.RecipientType.TO,
                InternetAddress.parse(emailMessage.getToAddress()));
            // Subject
            message.setSubject(emailMessage.getSubject());
            // Reply-To
            if (!StringUtils.isEmpty(emailMessage.getReplyToAddress())) {
                message.setReplyTo(InternetAddress.parse(emailMessage
                    .getReplyToAddress()));
            }

            // Body, plain vs. html
            MimeMultipart multiPartContent = new MimeMultipart();

            if (!StringUtils.isEmpty(emailMessage.getPlainBody())) {
                MimeBodyPart plain = new MimeBodyPart();
                plain.setText(emailMessage.getPlainBody());
                multiPartContent.addBodyPart(plain);
            }
            if (!StringUtils.isEmpty(emailMessage.getHtmlBody())) {
                MimeBodyPart html = new MimeBodyPart();
                html.setContent(emailMessage.getHtmlBody(), "text/html");
                multiPartContent.addBodyPart(html);
                multiPartContent.setSubType("alternative");
            }
            message.setContent(multiPartContent);

            // Ship it
            Transport.send(message);
            logger.info("Sent email to [{}] with subject [{}].", 
                emailMessage.getToAddress(), emailMessage.getSubject());

        } catch (MessagingException me) {
            logger.warn("Failed to send: [{}]", 
                me.getMessage());
            me.printStackTrace();
            throw new RuntimeException(me);
        }
    }
    
    
    // --- SettingsBackedObject support ---
    
    /**
     * Called to initialize this stmp mail provider using the given {@code settings}.
     */    
    @Override
    public void initialize(String settingsKey, Map<String, Object> settings) {
        this.setHost(ObjectUtils.to(String.class, settings.get("host")));
        this.setUsername(ObjectUtils.to(String.class, settings.get("username")));
        this.setPassword(ObjectUtils.to(String.class, settings.get("password")));
        
        Object useTLS = settings.get("useTLS");
        if (useTLS != null) {
            this.setUseTLS(ObjectUtils.to(Boolean.class, useTLS));
            Object tlsPort = settings.get("tlsPort");
            if (tlsPort != null) {
                this.setTlsPort(ObjectUtils.to(Long.class, tlsPort));
            }
        }
        
        Object useSSL = settings.get("useSSL");
        if (useSSL != null) {
            this.setUseSSL(ObjectUtils.to(Boolean.class, useSSL));
            Object sslPort = settings.get("sslPort");
            if (sslPort != null) {
                this.setSslPort(ObjectUtils.to(Long.class, sslPort));
            }
        }
    }
    
}
