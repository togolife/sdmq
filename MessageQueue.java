package sdmq;
import java.io.*;
import java.util.*;

public class MessageQueue {
	private Deque<Message> ready = new LinkedList<Message>();     // 就绪消息
	private LinkedList<Long> unacked = new LinkedList<Long>();   // 未确认消息
	private int dataDurable = 1;
	private long msgid = 0;
	private String exchangeName;
	private String keyName;
	private String name;
	
	public MessageQueue(String exchange, String key, String name) {
		this.exchangeName = exchange;
		this.keyName = key;
		this.name = name;
		File f = new File("./queues/" + exchange + key + name);
		if (f.exists()) {
			long maxid = 0;
			try {
				BufferedReader br = new BufferedReader(new FileReader(f));
				String msg;
				while ((msg = br.readLine()) != null) {
					String[] infos = msg.split("\t");
					long tmpid = Long.parseLong(infos[0]);
					ready.addLast(new Message(infos[1], tmpid));
					if (tmpid > maxid) maxid = tmpid;
				}
				br.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
				if (maxid == 0) {
					Random r = new Random();
					msgid = r.nextInt(1000000);
				} else {
					msgid = maxid + 1;
				}
			}
		} else {
			Random r = new Random();
			msgid = r.nextInt(1000000);
		}
	}
	
	public MessageQueue(String exchange, String key, String name, int dataDurable) {
		this(exchange, key, name);
		this.dataDurable = dataDurable;
	}
	
	public void addMessage(String mes) {
		if (++msgid >= 1000000) {
			msgid = 0;
		}
		String msgseq = exchangeName + keyName + name;
		Message message = new Message(mes, msgid);
		if (dataDurable == 1) {
			try {
				File f = new File("./queues/");
				if (!f.exists()) {
					f.mkdirs();
				}
				FileWriter fileWriter = new FileWriter("./queues/" + msgseq, true);
				fileWriter.write(msgid + "\t" + mes + "\n");
				fileWriter.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		ready.addLast(message);
	}
	
	public Message getMessage() {
		try {
			Message message = ready.removeFirst();
			if (message != null) {
				unacked.addLast(message.getMessageID());
			}
			return message;
		} catch (Exception e) {
			return null;
		}
	}
	
	public boolean ackMessage(String msgid) {
		String msgseq = exchangeName + keyName + name;
		Long id = Long.parseLong(msgid);
		if (unacked.remove(id)) {
			if (dataDurable == 1) {
				try {
					FileWriter fileWriter = new FileWriter("./queues/" + msgseq + ".2");
					BufferedReader br = new BufferedReader(new FileReader("./queues/" + msgseq));
					String msg;
					while ((msg = br.readLine()) != null) {
						if (!msg.split("\t")[0].equals(msgid)) {
							fileWriter.write(msg + "\n");
						}
					}
					br.close();
					fileWriter.close();
					File f = new File("./queues/" + msgseq);
					f.delete();
					f = new File("./queues/" + msgseq + ".2");
					f.renameTo(new File("./queues/" + msgseq));
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		return true;
	}
}
