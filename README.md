smss(small message send system),一个小型的消息队列。

# 概览

它是一个小型的消息队列，它占用的资源很少，运维简单，但处理的消息的总量、性能与其他的大型mq相比会差很多，比如kafka、rocketMQ，因此，它只能被应用非大型的业务场景中，适合创业初期，业务量不大，且机器资源有限的场景。但它也具备自己的特点：
* 占用资源非常少，在消息量少的情况下，比如qps几千，仅仅需要几十m，最多不到100m内存
* 持久化，对于某些B端业务，持久化是个强需求
* topic支持生命周期，某些场景需要一些临时的topic，频繁的删除topic会让业务代码变得更复杂，创建topic时给一个生命周期，生命周期结束后资源自动回收
* 主从复制，提供了类似mysql的主从复制，保证ha
* 还算不错的性能，单机测试，256线程，每线程10万请求，每请求200字节，qps可以达到10+万(ubuntu, 16G,20核测试)

# 部署

smss的部署非常简单，编译后只有一个可执行文件，再配以一个配置文件即可。一般部署目录如下：

* config, 该目录下是config.yaml, 描述各种配置参数
* data， 数据存储目录
* smss， 服务程序

## 配置文件——config.yaml

| 配置项                             | 描述                                                                         | 
|:--------------------------------|:---------------------------------------------------------------------------|
| port                            | 服务器端口                                                                      |
| log.path                        | log文件的输出路径，可以是绝对路径，也可以是相对路径，还可以是 stdout, stdout表示输出到控制台上                   |
| log.sample                      | 发布消息、管理mq命令的日志输出的采样率，每隔多少条输出一次日志，>=0, ==0表示不输出日志，== 1，每条都输出                |
| store.path                      | 消息数据的存储路径，一般设置为data，即在当前目录下的data子目录中存储数据                                   |
| store.maxLogSize                | 每个数据存储文件的大小，一般设置为1G,用字节数表示                                                 |
| store.maxDays                   | 数据文件存活的最大天数，超过这个天数，文件会被删除                                                  |
| store.clearInterval             | 数据回收线程的扫描间隔，即每隔这么久时间唤醒扫描一次，单位是s                                            |
| store.waitDelLockTimeoutMs      | 回收数据文件时，需要获取该文件的保护锁，这个配置表示等待锁的时间，单位ms，一般不需要改动                              |
| worker.buffSize                 | smss采用单线程持久化数据，该单线程称之为worker， buffSize即等待worker处理的任务的个数，一般不需要改动            |
| worker.waitMsgTimeout           | worker等待新的命令的超时时长，单位ms，超过该时长，worker也会唤醒，唤醒后会打印日志                           |
| worker.waitMsgTimeoutLogSample  | worker等待新命令超时后日志打印输出的采样率,连续超时唤醒 waitMsgTimeoutLogSample次后，打印一条日志           |
| timeout.net.write               | smss向client端输出时的超时，单位ms                                                    |
| time.server.alive               | 在client订阅消息时，当一直没有消息时会给订阅端发送server还活着的消息，当超过time.server.alive这么久没消息时会发送    |
| background.life.defaultScanSec  | 扫描有生命周期的topic的线程在无任何有生命周期的topic的情况下，也需要被唤醒，defaultScanSec指明这个唤醒间隔，单位是s     |
| background.delay.firstExec | 延迟消息也需要一个线程，按时唤醒， firstExec指明smss启动后第一次被唤醒的时机，即启动firstExec后，执行一次延迟消息扫描，单位s |

## master部署

``
  ./smss -role master
``

role,表示以什么角色来启动实例，角色包括master和slave

## slave部署

``
./smss -role slave -host 127.0.0.1 -port 12301 -event 0
``

event, smss的设计里，每一个写操作（包括发布消息、创建、删除mq，）都有一个eventId，eventId是唯一且递增的，根据这个eventId可以定位到哪个数据文件的哪个位置。
event参数表示slave已经复制完的事件，master需要发送下一个事件的数据，当event设置为0时，表示master需要从它的第一个文件的、第0个字节开始发送数据。

