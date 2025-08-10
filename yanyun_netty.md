
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
                                          --- io.netty.channel.nio.NioEventLoop.processSelectedKeysOptimized();  EventLoop中run方法执行的 （该方法netty做了性能优化，其底层就是把原来的set换成数组，减少哈希计算和冲突）   
                                            --- io.netty.channel.nio.NioEventLoop.processSelectedKey(java.nio.channels.SelectionKey, io.netty.channel.nio.AbstractNioChannel)
                                                ps；该方法中当读事件响应时候，就会调用unsafe.read()，又重新回到AbstracUnsafe中，然后由子类进行事件的处理
                                                
                                                if ((readyOps & (SelectionKey.OP_READ | SelectionKey.OP_ACCEPT)) != 0 || readyOps == 0) {
                                                      unsafe.read();
                                                 }
                                              --- io.netty.channel.nio.AbstractNioByteChannel.NioByteUnsafe#read  这个方法中，就是调用了AbstractNioByteChannel的doReadBytes方法
                                                  ps:
                                                  该方法中，会先调用AbstractNioByteChannel的allocate方法，通过ByteBufAllocator分配具体的ByteBuf，具体的实例对象为PooledByteBufAllocator，
                                                  底层用 jemalloc 风格的 chunk/page/subpage 三级结构 管理堆外内存,可通过 -Dio.netty.allocator.type=unpooled 或 channel.config().setAllocator(...) 切换.Handle 是“一次读循环”的上下文，保存了本次应该分配多大的 buffer、上轮读了多少字节、是否继续读等状态。
                                                  4.2 以后默认实现是 AdaptiveRecvByteBufAllocator.HandleImpl；
                                                  它内部维护 指数回退表：64 → 128 → 256 … → 65536（可配置上限），根据 实际读到的字节数 动态调整下一次大小，避免 “大马拉小车” 或 “小马拉大车”。

                                                   
                                                      final ByteBufAllocator allocator = config.getAllocator();
                                                      final RecvByteBufAllocator.Handle allocHandle = recvBufAllocHandle();
                                                      byteBuf = allocHandle.allocate(allocator);

                                                       
                                                    
                                            






```