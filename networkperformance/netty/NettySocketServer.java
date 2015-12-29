package networkperformance.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledUnsafeDirectByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.ReferenceCountUtil;
import networkperformance.DataGen;
import networkperformance.ServerClient;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Created by julyhou on 15/12/25.
 */

// actually we should cache the message and wait unitl they could form one whole message
class ServerWorkerHandler extends ChannelInboundHandlerAdapter {
  int count = 0;
  int times = DataGen.TotalDataSize;
  long start = System.currentTimeMillis();

  @Override // actually do nothing
  public void channelRead(ChannelHandlerContext context, Object message){
    ByteBuf data = (ByteBuf) message;
    count ++;
    times -= data.readableBytes();
    ReferenceCountUtil.release(message);
  }

  @Override
  public void channelReadComplete(ChannelHandlerContext context) {

    if(times == 0) {
      long end = System.currentTimeMillis();
      context.channel().close();
      context.close();
      System.out.printf("%d %d %d %d\n", start, end, end - start, count);
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext context, Throwable cause){
    cause.printStackTrace();
    context.close();
  }
}

public class NettySocketServer implements ServerClient{
  NioEventLoopGroup serverGroup;
  ServerBootstrap serverBP;

  public void init() throws IOException {
    serverGroup = new NioEventLoopGroup();
    serverBP = new ServerBootstrap();
    serverBP.group(serverGroup)
      .channel(NioServerSocketChannel.class)
      .localAddress(new InetSocketAddress(DataGen.masterPort))
      .childHandler(new ChannelInitializer<SocketChannel>() {
        @Override
        protected void initChannel(SocketChannel socketChannel) throws Exception {
          socketChannel.pipeline().addLast(new ServerWorkerHandler());
        }
      });
  }

  @Override
  public void start() {
    try {
      ChannelFuture future = serverBP.bind().sync();
      future.channel().closeFuture().sync();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }finally {
      try {
        serverGroup.shutdownGracefully().sync();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    System.out.println("Server exited");
  }

  public static void main(String[] args){
    NettySocketServer server = new NettySocketServer();
    try {
      server.init();
      server.start();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

}
