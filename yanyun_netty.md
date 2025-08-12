
##  ChannelFuture cf = bootstrap.bind(6668).sync()  调用栈

```
ServerBootstarp中的bind方法
  -- AbstractBootstrap 抽象类中的bind方法
     --- io.netty.bootstrap.AbstractBootstrap#bind()
       --- io.netty.bootstrap.AbstractBootstrap#doBind()
          以下分两步：
             --- 先执行 io.netty.bootstrap.AbstractBootstrap#initAndRegister()
                 以下也分两步：
                  --- io.netty.bootstrap.ChannelFactory#newChannel()  初始化个Channle出来，NioServerSocketChannel,OioServerSocketChannel等... 
                     --- io.netty.channel.ReflectiveChannelFactory 底层通过反射，其实就是把配置中的写的NioServerSocketChannel.class进行实例化
                  --- io.netty.channel.EventLoopGroup#register(io.netty.channel.Channel); 将Channel和EventLoop线程绑定起来
                     --- io.netty.channel.SingleThreadEventLoop#register(io.netty.channel.ChannelPromise) 调用EventLoop的register
                       --- io.netty.channel.Channel.Unsafe#register 表面上看是EventLoop在注册，其底层其实是调用绑定的Channel中的register
                         ---io.netty.channel.AbstractChannel.AbstractUnsafe#register 然后Channel又调用AbstractUnsafe内部抽象类的register
                                   ps:
                                   该饭饭做了一系类的检查操作，包括设计Promise，而且这一步有个关键的点：
                                             AbstractChannel.this.eventLoop = eventLoop;
                                   真正eventloop赋值给AbstractChannel的成员变量中，真正把eventloop和channel绑定起来
                                   而且
                                             eventLoop.execute(()-> {register0(promise); });
                                   这个启动EventLoop线程！！！就是再这一步执行的
                              --- io.netty.channel.AbstractChannel.AbstractUnsafe#register0 然后又调用的register0这个方法
                                   ps:
                                    该方法做一些回调通知操作：
                                    
                                       pipeline.invokeHandlerAddedIfNeeded();  通知调用方进行HandlerAdd回调
                                       pipeline.fireChannelRegistered();   回调Channel注册回调
                                       
                                 --- io.netty.channel.AbstractChannel.AbstractUnsafe.beginRead 接着就开始执行数据读取
                                   --- io.netty.channel.AbstractChannel.doBeginRead 还是由AbstractChanel中的Unsafe子类去执行  
                                     --- io.netty.channel.nio.AbstractNioChannel.doBeginRead 这个时候就来到了子类AbstractNioChannel中
                                         ps:
                                          该方法将读事件注册到Selector上：
                                          
                                             final int interestOps = selectionKey.interestOps();
                                               if ((interestOps & readInterestOp) == 0) {
                                                    selectionKey.interestOps(interestOps | readInterestOp);
                                               }
                                               
                                            然后当读事件响应的时候，又哪一个方法进行执行处理的呢，答案是：EventLoop中的run方法，会不断进入死循环判断事件的响应类型
                                          --- io.netty.channel.nio.NioEventLoop.processSelectedKeysOptimized();  EventLoop中run方法执行的 
                                              （该方法netty做了性能优化，其底层就是把原来的set换成数组，减少哈希计算和冲突）   
                                            --- io.netty.channel.nio.NioEventLoop.processSelectedKey(java.nio.channels.SelectionKey, io.netty.channel.nio.AbstractNioChannel)
                                                ps；该方法中当读事件响应时候，就会调用unsafe.read()，又重新回到AbstracUnsafe中，然后由子类进行事件的处理
                                                
                                                if ((readyOps & (SelectionKey.OP_READ | SelectionKey.OP_ACCEPT)) != 0 || readyOps == 0) {
                                                      unsafe.read();
                                                 }
                                                 
                                              <<<< 处理读事件 >>>>  
                                              --- io.netty.channel.nio.AbstractNioByteChannel.NioByteUnsafe#read  这个方法中，就是调用了AbstractNioByteChannel的doReadBytes方法
                                                  ps:
                                                  该方法中，会先调用AbstractNioByteChannel的allocate方法，通过ByteBufAllocator分配具体的ByteBuf，具体的实例对象为PooledByteBufAllocator，
                                                  底层用 jemalloc 风格的 chunk/page/subpage 三级结构 管理堆外内存,可通过 -Dio.netty.allocator.type=unpooled 
                                                  或 channel.config().setAllocator(...) 切换.Handle 是“一次读循环”的上下文，保存了本次应该分配多大的 buffer、上轮读了多少字节、是否继续读等状态。
                                                  4.2 以后默认实现是 AdaptiveRecvByteBufAllocator.HandleImpl；
                                                  它内部维护 指数回退表：64 → 128 → 256 … → 65536（可配置上限），根据 实际读到的字节数 动态调整下一次大小，避免 “大马拉小车” 或 “小马拉大车”。

                                                   
                                                      final ByteBufAllocator allocator = config.getAllocator();
                                                      final RecvByteBufAllocator.Handle allocHandle = recvBufAllocHandle();
                                                      byteBuf = allocHandle.allocate(allocator);
                                                 
                                                 --- io.netty.channel.nio.AbstractNioByteChannel.doReadBytes 接着调用doReadBytes方法, 把内核数据搬进 ByteBuf 
                                                    ps:
                                                      NIO实现:
                                                        - 抽象层：SocketChannel.read(byteBuf.internalNioBuffer(...))
                                                        - 底层：read(2)  →  Linux sys_read(fd, user_buf, len)
                                                      Epoll/IO_URING 实现:
                                                        - epoll_wait 返回后 → recvmsg(iovec) 直接写进 pooled memory（中间省掉一次  ByteBuffer  中间拷贝）
                                                      之后调用：
                                                        allocHandle.lastBytesRead(localReadAmount);
                                                        allocHandle.readComplete();   // 更新指数表，如果本次读满，下次  guess  会翻倍；没读满就回退到更小档。
                                                      
                                                      
                                                    
                                                  --- io.netty.channel.ChannelPipeline.fireChannelRead() 之后调用fireChannelRead方法，由channelPiple管道进行数据传播，
                                                        从 HeadContext 开始，依次调用每个  ChannelInboundHandler.channelRead()     
                                                   --- io.netty.channel.ChannelPipeline.fireChannelReadComplete() 接着调用fireChannelReadComplete方法，由channelPiple管道进行数据传播
                                              
                                              <<< 处理写事件 >>>
                                              --- io.netty.channel.nio.AbstractNioChannel.NioUnsafe.forceFlush() 
                                               --- io.netty.channel.AbstractChannel.AbstractUnsafe.flush0 最终会调用flush0这个方法，处于AbstractChannel中
                                               
                                                 ps：执行流程如下
                                                   用户线程
                                                      └── ctx.writeAndFlush(msg)  → 进入 pipeline
                                                      └── HeadContext.write → 把 msg 放进 ChannelOutboundBuffer
                                                      └── HeadContext.flush → unsafe.flush()
                                                      └── EventLoop OP_WRITE 就绪 → unsafe.forceFlush()
                                                           ├── 1. flush0() 将 outboundBuffer 里的 ByteBuf 全部写进 JDK SocketChannel
                                                           ├── 2. 如果还有剩余，继续监听 OP_WRITE
                                                           └── 3. 如果写完，清除 OP_WRITE，防止 CPU 空转

                                                    --- io.netty.channel.AbstractChannel.doWrite 接着执行doWrite方法，该方法处于AbstractChannel中，由子类进行重写   
                                                        --- io.netty.channel.nio.AbstractNioByteChannel.doWrite  取出对头消息，连续写 16 次还没写完 → 说明内核缓冲区满了，先让出 CPU，等下一次 OP_WRITE 事件再继续。
                                                          --- io.netty.channel.nio.AbstractNioByteChannel.doWriteInternal 执行doWriteInternal, 主要把object对象转为ByteBuf对象
                                                            ps: 
                                                               doWriteInternal  内部：
                                                                  - 如果obj的类型为 ByteBuf  →  doWriteBytes(buf)  →  SocketChannel.write(ByteBuffer) 
                                                                  - 如果obj的类型为 FileRegion  →  doWriteFileRegion(region)  →  transferTo  零拷贝
                                                                  - 如果obj的类型不为上述两种类型 → 抛出异常 
                                                                  以下以ByteBuf类型为例继续执行调用栈
                                                               --- io.netty.channel.nio.AbstractNioByteChannel.doWriteBytes 来到doWriteBytes，由具体子类NioSocketChannel负责执行
                                                                --- io.netty.channel.socket.nio.NioSocketChannel.doWriteBytes   
                                                                  ps:
                                                                    buf.readBytes(javaChannel(),buf.readableBytes())
                                                                  --- 
                                                                 
                                                                
                                                            
                                             
                                                       
                                                    
                                            






```