# sdmq
消息中间件简单实现

仿照rabbitmq设计，由交换机(exchange)，路由(key)和队列(queue)三部分组成。
生产者将消息发送到交换机，mq负责依据路由转发到队列中。如果没有路由，也没有队列，那么生产者发送消息丢弃；
消费者建立通道，通道绑定交换机、路由和队列，然后通过主动请求或者被动接收的方式获取消息。

消息格式："消息长度|消息内容"

参数：

1.交换机

a.名称

b.是否持久  持久启动时候就会加载交换机

  可选参数，默认是不持久
  
2.队列

a.名称

b.交换机

c.路由

d.队列是否持久

  可选参数，默认不持久
  
e.数据是否持久

  可选参数，默认不持久
  

返回代码：

200 成功
201 失败  通用失败
202 失败  未登录，不允许操作
203 登录失败 用户名密码不对
204 交换机不存在
205 没有绑定队列
206 没有消息

1.登录

请求：ACTION=login,USER=username,PASSWORD=password

响应：RETCODE=200  203

2.建立交换机

请求：ACTION=create-exchange,EXCHANGE=exchange-name,DURABLE=false

响应：RETCODE=200  202

3.建立队列

请求：ACTION=create-queue,QUEUE=queue-name,EXCHANGE=exchange-name,KEY=key-name,DURABLE=false,DATA-DURABLE=false

响应：RETCODE=200  202 204

4.发送消息

请求：ACTION=publish,EXCHANGE=exchange-name,KEY=key-name,MSG=msg-content

响应：RETCODE=200   202 204

5.请求消息

请求：ACTION=get,QUEUE=queue-name,EXCHANGE=exchange-name,KEY=key-name

响应：RETCODE=200,MSGID=id,MSG=msg-content   202 204 205 206

6.注册拉取消息

请求：ACTION=poll,QUEUE=queue-name,EXCHANGE=exchange-name,KEY=key-name

响应：RETCODE=200  202 204 205

7.确认消息

请求：ACTION=confirm,MSGID=id

响应：RETCODE=200

8.推送消息

响应：MSGID=id,MSG=msg-content
