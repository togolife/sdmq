package sdmq;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.*;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.net.*;

class MsgBuffer {
	private ByteBuffer readBuffer;
	private ByteBuffer writeBuffer;
	private Charset charset = Charset.forName("GBK");
	
	public MsgBuffer() {
		readBuffer = ByteBuffer.allocate(1024);
		writeBuffer = ByteBuffer.allocate(1024);
	}
	
	public void pushBuffer(int rw, ByteBuffer buff) {
		ByteBuffer buffer = (rw == RD ? readBuffer : writeBuffer);
		buffer.limit(buffer.capacity());
		relarge(rw, buff.limit());
		buffer = (rw == RD ? readBuffer : writeBuffer);
		buffer.put(buff);
	}
	
	public void pushMsg(int rw, String msg) {
		String writeMsg = msg.length() + "|" + msg;
		ByteBuffer tmp = encode(writeMsg);
		pushBuffer(rw, tmp);
	}
	
	public String getReadMsg() {
		readBuffer.flip();
		String msg = decode(readBuffer);
		String ret = null;
		int index = msg.indexOf('|');
		if (index != -1) {
			int len = Integer.parseInt(msg.substring(0, index));
			if (msg.length() >= index + 1 + len) {
				msg = msg.substring(0, index+1+len);
				readBuffer.position(encode(msg).limit());
				readBuffer.compact();
				resize(RD);
				ret = msg.substring(index+1);
			}
		}
		if (ret == null) {
			readBuffer.position(readBuffer.limit());
		}
		return ret;
	}
	
	public ByteBuffer getWriteMsg() {
		return writeBuffer;
	}
	
	private void relarge(int rw, int len) {
		ByteBuffer buffer = (rw == RD ? readBuffer : writeBuffer);
		int remain = buffer.remaining();
		if (remain > len)
			return;
		while (remain < len) {
			remain += 1024;
		}
		ByteBuffer tmp = ByteBuffer.allocate(remain);
		tmp.put(buffer);
		if (rw == RD) {
			readBuffer = tmp;
		} else {
			writeBuffer = tmp;
		}
	}
	
	private void resize(int rw) {
		ByteBuffer buffer = (rw == RD ? readBuffer : writeBuffer);
		if (buffer.capacity() > 1024) {
			ByteBuffer tmp = ByteBuffer.allocate(1024);
			tmp.put(buffer);
			if (rw == RD) {
				readBuffer = tmp;
			} else {
				writeBuffer = tmp;
			}
		}
	}
	
	private String decode(ByteBuffer buffer) {
		CharBuffer charBuffer = charset.decode(buffer);
		return charBuffer.toString();
	}
	
	private ByteBuffer encode(String str) {
		return charset.encode(str);
	}
	
	public static final int RD = 0;
	public static final int WR = 1;
}

public class MQServer {
	private ServerSocketChannel serverSocketChannel;
	private ServerSocket serverSocket;
	private Selector selector;
	private Map<String, Exchange> exchanges = new HashMap<String, Exchange>();
	private Map<SocketChannel,Integer> links = new HashMap<SocketChannel,Integer>();
	private Map<SocketChannel,MessageQueue> consumers = new HashMap<SocketChannel,MessageQueue>();
	private Log log;
	private Property property;
	
	public MQServer(String host, int port) throws Exception {
		log = new Log(this.getClass().getName(), 200 * 1024 * 1024);
		property = new Property();
		if (!property.load()) {
			log.print("加载配置信息失败!");
			throw new Exception("load property failed!");
		}
		for (String s : property.getExchange()) {
			Exchange exchange = new Exchange(s);
			exchanges.put(s, exchange);
		}
		for (Entry<String, QueueProperty> e : property.getQueue().entrySet()) {
			String queueName = e.getKey();
			QueueProperty qp = e.getValue();
			String exchangeName = qp.exchange;
			String keyName = qp.key;
			int dataDurable = qp.dataDurable;
			if (!exchanges.containsKey(exchangeName)) {
				log.print("队列" + queueName + "所属的交换机" + exchangeName + "不存在!");
				throw new Exception("queue's exchange is not exists!");
			}
			exchanges.get(exchangeName).addQueue(keyName, queueName, dataDurable);
		}
		serverSocketChannel = ServerSocketChannel.open();
		serverSocket = serverSocketChannel.socket();
		serverSocket.setReuseAddress(true);
		serverSocketChannel.configureBlocking(false);
		InetAddress ia = InetAddress.getByName(host);
		InetSocketAddress isa = new InetSocketAddress(ia,port);
		serverSocket.bind(isa);
		selector = Selector.open();
		log.print("服务启动成功！");
	}
	
	public void service() throws IOException {
		serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
		while (selector.select() > 0) {
			Set<SelectionKey> readyKeys = selector.selectedKeys();
			Iterator<SelectionKey> it = readyKeys.iterator();
			while (it.hasNext()) {
				SelectionKey key = null;
				try {
					key = (SelectionKey) it.next();
					it.remove();
					if (key.isAcceptable()) {
						ServerSocketChannel ssc = (ServerSocketChannel)key.channel();
						SocketChannel socketChannel = ssc.accept();
						log.print("接收到客户连接，来自:" + socketChannel.socket().getInetAddress() +
								":" + socketChannel.socket().getPort());
						socketChannel.configureBlocking(false);
						if (!socketChannel.socket().getKeepAlive()) {  // keepalive 要验证一下
							socketChannel.socket().setKeepAlive(true);
						}
						MsgBuffer buffer = new MsgBuffer();
						socketChannel.register(selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE, buffer);
						links.put(socketChannel, 0);
					}
					if (key.isWritable()) {
						send(key);
					}
					if (key.isReadable()) {
						receive(key);
					}
				} catch (IOException e) {
					e.printStackTrace();
					if (key != null) {
						try {
							key.cancel();
							key.channel().close();
						} catch (Exception ex) {
							ex.printStackTrace();
						}
					}
				}
			}
		}
	}
	
	public void send(SelectionKey key) throws IOException {
		SocketChannel socketChannel = (SocketChannel) key.channel();
		MsgBuffer buffer = (MsgBuffer) key.attachment();
		ByteBuffer writeBuffer = buffer.getWriteMsg();
		writeBuffer.flip();
		int type = 0;
		if (links.get(socketChannel) != null) {
			type = links.get(socketChannel);
		}
		
		if (writeBuffer.hasRemaining()) {
			while (writeBuffer.hasRemaining()) {
				socketChannel.write(writeBuffer);
			}
			writeBuffer.position(writeBuffer.limit());
			writeBuffer.compact();
		} else if (type == 2) { // type = 2 是消息消费者
			MessageQueue queue = consumers.get(socketChannel);
			Message message = queue.getMessage();
			if (message != null) {
				buffer.pushMsg(MsgBuffer.WR, "MSGID=" + message.getMessageID()+",MSG="+message.getMessage());
			}
		}
	}
	
	public void receive(SelectionKey key) throws IOException {
		MsgBuffer buffer = (MsgBuffer) key.attachment();
		SocketChannel socketChannel = (SocketChannel) key.channel();
		ByteBuffer tmpReadBuffer = ByteBuffer.allocate(256);
		int n = socketChannel.read(tmpReadBuffer);
		if (n == -1) {
			key.cancel();
			socketChannel.close();
			links.remove(socketChannel);
			log.print("客户端主动关闭，来自:" + socketChannel.socket().getInetAddress() +
					":" + socketChannel.socket().getPort());
			return;
		}
		if (n == 0) {
			int x = key.interestOps();
			if ((x & ~SelectionKey.OP_READ) == 0) {
				key.cancel();
				socketChannel.close();
				links.remove(socketChannel);
				log.print("关闭客户连接，来自:" + socketChannel.socket().getInetAddress() +
						":" + socketChannel.socket().getPort());
			} else {
				x &= ~SelectionKey.OP_READ;
				key.interestOps(x);
			}
			return;
		}
		tmpReadBuffer.flip();
		buffer.pushBuffer(MsgBuffer.RD, tmpReadBuffer);
		String msg = buffer.getReadMsg();
		if (msg == null) {
			return;
		}
		if (msg.indexOf(',') == -1) {
			buffer.pushMsg(MsgBuffer.WR, "RETCODE=201,REGMSG=传入消息格式不对");
			return;
		}
		// 消息格式  key=value,key=value
		Map<String,String> kv = new HashMap<String,String>();
		for (String s : msg.split(",")) {
			s = s.trim();
			String[] requestInfo = s.split("=");
			if (requestInfo.length != 2) {
				buffer.pushMsg(MsgBuffer.WR, "RETCODE=201,RETMSG=传入消息格式不对");
				return;
			}
			kv.put(requestInfo[0].trim(), requestInfo[1].trim());
		}
		if (!kv.containsKey("ACTION")) {
			buffer.pushMsg(MsgBuffer.WR, "RETCODE=201,RETMSG=传入消息格式不对");
			return;
		}
		String action = kv.get("ACTION");
		if (action.equals("login")) {
			if (kv.containsKey("USER") && kv.containsKey("PASSWORD")) {
				String user = kv.get("USER");
				String passwd = kv.get("PASSWORD");
				Map<String,String> mq_property = property.getProperties();
				if (user.equals(mq_property.get("user")) && passwd.equals(mq_property.get("passwd"))) {
					links.put(socketChannel, 1);
					buffer.pushMsg(MsgBuffer.WR, "RETCODE=200");
				} else {
					buffer.pushMsg(MsgBuffer.WR, "RETCODE=203,RETMSG=用户名密码不对");
				}
			} else {
				buffer.pushMsg(MsgBuffer.WR, "RETCODE=201,RETMSG=请传入用户名密码");
			}
		} else if (action.equals("create-exchange")) {
			if (links.get(socketChannel) != 1) {
				buffer.pushMsg(MsgBuffer.WR, "RETCODE=202,RETMSG=当前状态不允许操作");
			} else if (kv.containsKey("EXCHANGE")) {
				String exchangeName = kv.get("EXCHANGE");
				int durable = 0;
				if (kv.containsKey("DURABLE")) {
					if (kv.get("DURABLE").equals("true")) {
						durable = 1;
					}
				}
				if (!exchanges.containsKey(exchangeName)) {
					Exchange exchange = new Exchange(exchangeName);
					exchanges.put(exchangeName, exchange);
					if (durable == 1) {
						property.addExchange(exchangeName);
						property.save();
					}
				}
				buffer.pushMsg(MsgBuffer.WR, "RETCODE=200");
			} else {
				buffer.pushMsg(MsgBuffer.WR, "RETCODE=201,RETMSG=传入建立交换机参数不完整");
			}
		} else if (action.equals("create-queue")) {
			if (links.get(socketChannel) != 1) {
				buffer.pushMsg(MsgBuffer.WR, "RETCODE=202,RETMSG=当前状态不允许操作");
			} else if (kv.containsKey("QUEUE") && kv.containsKey("EXCHANGE") && kv.containsKey("KEY")) {
				String queueName = kv.get("QUEUE");
				String exchangeName = kv.get("EXCHANGE");
				String keyName = kv.get("KEY");
				int durable = 0, dataDurable = 0;
				if (kv.containsKey("DURABLE") && kv.get("DURABLE").equals("true")) {
					durable = 1;
				}
				if (kv.containsKey("DATA-DURABLE") && kv.get("DATA-DURABLE").equals("true")) {
					dataDurable = 1;
				}
				if (!exchanges.containsKey(exchangeName)) {
					buffer.pushMsg(MsgBuffer.WR, "RETCODE=204,RETMSG=交换机不存在");
				} else {
					exchanges.get(exchangeName).addQueue(keyName, queueName, dataDurable);
					if (durable == 1) {
						property.addQueue(queueName, exchangeName, keyName, dataDurable);
						property.save();
					}
					buffer.pushMsg(MsgBuffer.WR, "RETCODE=200");
				}
			} else {
				buffer.pushMsg(MsgBuffer.WR, "RETCODE=201,RETMSG=传入建立队列参数不完整");
			}
		} else if (action.equals("publish")) {
			if (links.get(socketChannel) != 1) {
				buffer.pushMsg(MsgBuffer.WR, "RETCODE=202,RETMSG=当前状态不允许操作");
			} else if (kv.containsKey("EXCHANGE") && kv.containsKey("KEY") && kv.containsKey("MSG")) {
				String exchangeName = kv.get("EXCHANGE");
				String keyName = kv.get("KEY");
				String publishMsg = kv.get("MSG");
				if (!exchanges.containsKey(exchangeName)) {
					buffer.pushMsg(MsgBuffer.WR, "RETCODE=204,RETMSG=交换机不存在");
				} else {
					exchanges.get(exchangeName).addMessage(keyName, publishMsg);
					buffer.pushMsg(MsgBuffer.WR, "RETCODE=200");
				}
			} else {
				buffer.pushMsg(MsgBuffer.WR, "RETCODE=201,RETMSG=传入参数不完整");
			}
		} else if (action.equals("get")) {
			if (links.get(socketChannel) != 1) {
				buffer.pushMsg(MsgBuffer.WR, "RETCODE=202,RETMSG=当前状态不允许操作");
			} else if (kv.containsKey("EXCHANGE") && kv.containsKey("KEY") && kv.containsKey("QUEUE")) {
				String exchangeName = kv.get("EXCHANGE");
				String keyName = kv.get("KEY");
				String queueName = kv.get("QUEUE");
				if (!exchanges.containsKey(exchangeName)) {
					buffer.pushMsg(MsgBuffer.WR, "RETCODE=204,RETMSG=交换机不存在");
				} else {
					MessageQueue mq = exchanges.get(exchangeName).getQueue(keyName, queueName);
					if (mq == null) {
						buffer.pushMsg(MsgBuffer.WR, "RETCODE=205,RETMSG=队列不存在");
					} else {
						Message message = mq.getMessage();
						if (message == null) {
							buffer.pushMsg(MsgBuffer.WR, "RETCODE=206,RETMSG=没有消息");
						} else {
							consumers.put(socketChannel, mq);
							buffer.pushMsg(MsgBuffer.WR, "RETCODE=200,MSGID=" + message.getMessageID()+",MSG="+message.getMessage());
						}
					}
				}
			} else {
				buffer.pushMsg(MsgBuffer.WR, "RETCODE=201,RETMSG=传入参数不完整");
			}
		} else if (action.equals("poll")) {
			if (links.get(socketChannel) != 1) {
				buffer.pushMsg(MsgBuffer.WR, "RETCODE=202,RETMSG=当前状态不允许操作");
			} else if (kv.containsKey("EXCHANGE") && kv.containsKey("KEY") && kv.containsKey("QUEUE")) {
				String exchangeName = kv.get("EXCHANGE");
				String keyName = kv.get("KEY");
				String queueName = kv.get("QUEUE");
				if (!exchanges.containsKey(exchangeName)) {
					buffer.pushMsg(MsgBuffer.WR, "RETCODE=204,RETMSG=交换机不存在");
				} else {
					MessageQueue mq = exchanges.get(exchangeName).getQueue(keyName, queueName);
					if (mq == null) {
						buffer.pushMsg(MsgBuffer.WR, "RETCODE=205,RETMSG=队列不存在");
					} else {
						links.put(socketChannel, 2); // 消费者 consumer
						consumers.put(socketChannel, mq);
						buffer.pushMsg(MsgBuffer.WR, "RETCODE=200");
					}
				}
			} else {
				buffer.pushMsg(MsgBuffer.WR, "RETCODE=201,RETMSG=传入参数不完整");
			}
		} else if (action.equals("confirm")) {
			if (links.get(socketChannel) != 1 && links.get(socketChannel) != 2) {
				buffer.pushMsg(MsgBuffer.WR, "RETCODE=202,RETMSG=当前状态不允许操作");
			} else if (kv.containsKey("MSGID")) {
				MessageQueue mq = consumers.get(socketChannel);
				if (mq != null) {
					mq.ackMessage(kv.get("MSGID"));
				}
				buffer.pushMsg(MsgBuffer.WR, "RETCODE=200");
			} else {
				buffer.pushMsg(MsgBuffer.WR, "RETCODE=201,RETMSG=传入参数不完整");
			}
		} else {
			buffer.pushMsg(MsgBuffer.WR, "RETCODE=201,RETMSG=传入消息格式不对");
		}
	}

	public static void main(String[] args) {
		String host = "localhost";
		int port = 8080;
		if (args.length == 2) {
			host = args[1];
			port = Integer.parseInt(args[2]);
		}
		try {
			MQServer server = new MQServer(host, port);
			server.service();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return;
	}
}
