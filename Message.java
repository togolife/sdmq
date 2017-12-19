package sdmq;

public class Message {
	private String content;
	private long msgid;
	
	public Message(String content, long msgid) {
		this.content = content;
		this.msgid = msgid;
	}
	
	public String getMessage() {
		return content;
	}
	
	public long getMessageID() {
		return msgid;
	}
}
