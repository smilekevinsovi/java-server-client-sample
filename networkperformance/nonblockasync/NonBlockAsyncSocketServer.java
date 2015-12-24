package networkperformance.nonblockasync;

import networkperformance.DataGen;
import networkperformance.ServerClient;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class ServerAttachment {
  public AsynchronousServerSocketChannel channel;
}

class ServerWorkerAttachment{
  public int count = 0;
  public int times = 1;
}

class NonBlockAsyncSocketServerWorker implements NonBlockAsyncSocketServer.ServerWorker {
  private AsynchronousSocketChannel sc;
  public NonBlockAsyncSocketServerWorker(AsynchronousSocketChannel sc){
    this.sc = sc;
  }

  @Override
  public void run() {
    final long start = System.currentTimeMillis();
    final int totalRound = DataGen.TotalDataSize / DataGen.SendPerMessage;

    ServerWorkerAttachment attachment = new ServerWorkerAttachment();
    final ByteBuffer buffer = ByteBuffer.allocate(DataGen.SendPerMessage);

    sc.read(buffer, attachment, new CompletionHandler<Integer, ServerWorkerAttachment>() {
      @Override
      public void completed(Integer result, ServerWorkerAttachment attachment) {
        if(attachment.times == totalRound){
          long end = System.currentTimeMillis();
          System.out.printf("%d %d %d %d\n", start, end, end - start, attachment.count);
          return;
        }

        if(buffer.hasRemaining()){
          attachment.count ++;
          sc.read(buffer, attachment, this);
          //System.out.println("read partial");
        }else if(attachment.times < totalRound){
          attachment.times ++;
          buffer.clear();
          sc.read(buffer, attachment, this);
        }
      }

      @Override
      public void failed(Throwable exc, ServerWorkerAttachment attachment) {
        System.out.println("Read data error");
      }
    });
  }
}

public class NonBlockAsyncSocketServer implements ServerClient{
  AsynchronousServerSocketChannel asServerChannel = null;
  private ExecutorService pool;

  public interface ServerWorker extends Runnable {}

  public ServerWorker createWorker(AsynchronousSocketChannel sc) {

    return new NonBlockAsyncSocketServerWorker(sc);
  }

  @Override
  public void init() throws IOException {
    asServerChannel = AsynchronousServerSocketChannel.open();
    asServerChannel.bind(new InetSocketAddress(DataGen.masterPort));
    pool = Executors.newFixedThreadPool(DataGen.MasterThreadCount);
  }

  @Override
  public void start() {
    ServerAttachment attachment = new ServerAttachment();

    asServerChannel.accept(attachment, new CompletionHandler<AsynchronousSocketChannel, ServerAttachment>() {
        @Override
        public void completed(AsynchronousSocketChannel socketChannel, ServerAttachment attachment) {
          asServerChannel.accept(attachment, this);
          ServerWorker worker = createWorker(socketChannel);
          pool.execute(worker);
        }

        @Override
      public void failed(Throwable e, ServerAttachment attachment) {
        System.out.println("Connection fails");
        e.printStackTrace();
      }
    });

    try {
      Thread.currentThread().join();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args){
    NonBlockAsyncSocketServer server = new NonBlockAsyncSocketServer();

    try {
      server.init();
      server.start();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
