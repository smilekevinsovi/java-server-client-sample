package networkperformance.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoop;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import networkperformance.DataGen;
import networkperformance.ServerClient;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

/**
 * Created by julyhou on 15/12/25.
 */

class ClientWorkerHandler extends SimpleChannelInboundHandler<ByteBuf> {

  @Override
  public void channelActive(ChannelHandlerContext context) {
    ByteBuffer buffer = DataGen.genByte(DataGen.SendPerMessage);
    int count = DataGen.TotalDataSize / DataGen.SendPerMessage ;

    System.out.printf("send %dB per message with count %d\n", DataGen.SendPerMessage, count);
    buffer.flip();
    for(int i = 0; i < count; i ++){
      context.writeAndFlush(Unpooled.copiedBuffer(buffer));
    }
  }

  @Override
  protected void channelRead0(ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf) throws Exception {}

  @Override
  public void exceptionCaught(ChannelHandlerContext context, Throwable cause){
    cause.printStackTrace();
    context.close();
  }
}

public class NettySocketClient implements ServerClient{
  NioEventLoopGroup clientGroup;
  Bootstrap[] clientBPs = new Bootstrap[DataGen.ClientThreadCount];

  @Override
  public void init() throws IOException {
    clientGroup = new NioEventLoopGroup();
    for(int i = 0; i < DataGen.ClientThreadCount; i ++) {
      clientBPs[i] = new Bootstrap();
      clientBPs[i].group(clientGroup)
          .channel(NioSocketChannel.class)
          .remoteAddress(new InetSocketAddress(DataGen.masterHost, DataGen.masterPort))
          .handler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel socketChannel) throws Exception {
              socketChannel.pipeline().addLast(new ClientWorkerHandler());
            }
          });
    }
  }

  @Override
  public void start() {
    for(int i = 0; i < DataGen.ClientThreadCount; i ++) {
      try {
        ChannelFuture future = clientBPs[i].connect().sync();
        future.channel().closeFuture().sync();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }



  }

  public static void main(String[] args){
    NettySocketClient client = new NettySocketClient();
    try {
      client.init();
      client.start();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
