package com.psddev.dari.util;

/** Simple email message class with builder pattern support **/
public class MailMessage {

    private String toAddress;
    private String fromAddress;
    private String replyToAddress;
    private String subject;
    private String plainBody;
    private String htmlBody;

    public MailMessage(String toAddress) {
        this.toAddress = toAddress;
    }

    public static MailMessage to(String toAddress) {
        MailMessage msg = new MailMessage(toAddress);
        return msg;
    }

    public MailMessage from(String fromAddress) {
        this.fromAddress = fromAddress;
        return this;
    }

    public MailMessage replyTo(String replyToAddress) {
        this.replyToAddress = replyToAddress;
        return this;
    }

    public MailMessage subject(String subject) {
        this.subject = subject;
        return this;
    }

    public MailMessage plain(String plainBody) {
        this.plainBody = plainBody;
        return this;
    }

    public MailMessage html(String htmlBody) {
        this.htmlBody = htmlBody;
        return this;
    }

    public String getToAddress() {
        return toAddress;
    }

    public void setToAddress(String toAddress) {
        this.toAddress = toAddress;
    }

    public String getFromAddress() {
        return fromAddress;
    }

    public void setFromAddress(String fromAddress) {
        this.fromAddress = fromAddress;
    }

    public String getReplyToAddress() {
        return replyToAddress;
    }

    public void setReplyToAddress(String replyToAddress) {
        this.replyToAddress = replyToAddress;
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public String getPlainBody() {
        return plainBody;
    }

    public void setPlainBody(String plainBody) {
        this.plainBody = plainBody;
    }

    public String getHtmlBody() {
        return htmlBody;
    }

    public void setHtmlBody(String htmlBody) {
        this.htmlBody = htmlBody;
    }

    /**
     * Sends mail via MailProvider default, from settings.
     *
     * @return
     */
    public void send() {
        MailProvider.Static.getDefault().sendMail(this);
    }
}
