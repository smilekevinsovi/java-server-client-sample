package networkperformance.nonblockasync;

import com.sun.xml.internal.ws.api.message.Attachment;
import networkperformance.DataGen;
import networkperformance.ServerClient;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class ClientAttachment {}

class ClientWorkerAttachment {
  AsynchronousSocketChannel sc;
  int times = 1;
  int totalRound;
}

class NonBlockAsyncSocketClientWorker implements NonBlockAsyncSocketClient.Worker {

  @Override
  public void run() {
    AsynchronousSocketChannel sc = null;
    try {
      sc = AsynchronousSocketChannel.open();
    } catch (IOException e) {
      e.printStackTrace();
      return;
    }

    ClientWorkerAttachment attachment = new ClientWorkerAttachment();
    attachment.sc = sc;
    try {
      sc.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
    } catch (IOException e) {
      e.printStackTrace();
    }
    attachment.totalRound = DataGen.TotalDataSize / DataGen.SendPerMessage ;

    sc.connect(new InetSocketAddress(DataGen.masterHost, DataGen.masterPort), attachment,
      new CompletionHandler<Void, ClientWorkerAttachment>(){

        @Override
        public void completed(Void result, ClientWorkerAttachment attachment) {
          final ByteBuffer buffer = DataGen.genByte(DataGen.SendPerMessage);

          System.out.printf("send %dB per message with count %d\n", DataGen.SendPerMessage, attachment.totalRound);

          attachment.sc.write(buffer, attachment, new CompletionHandler<Integer, ClientWorkerAttachment>() {
            @Override
            public void completed(Integer result, ClientWorkerAttachment attachment) {
              if(buffer.hasRemaining()){
                attachment.sc.write(buffer, attachment, this);
              }else if(attachment.times < attachment.totalRound){
                attachment.times ++;
                buffer.flip();
                attachment.sc.write(buffer, attachment, this);
              }else {
                try {
                  attachment.sc.close();
                } catch (IOException e) {
                  e.printStackTrace();
                }
              }
            }

            @Override
            public void failed(Throwable exc, ClientWorkerAttachment attachment) {
              System.out.println("write values fails");
            }
          });
        }

        @Override
        public void failed(Throwable exc, ClientWorkerAttachment attachment) {
          System.out.println("connect server fails");
        }
      }
    );
  }
}

public class NonBlockAsyncSocketClient implements ServerClient{
  private AsynchronousSocketChannel channel = null;
  private ExecutorService pool;

  public interface Worker extends Runnable{}

  @Override
  public void init() throws IOException {
    pool = Executors.newFixedThreadPool(DataGen.ClientThreadCount);
  }

  @Override
  public void start() {
    for(int i = 0; i < DataGen.ClientThreadCount; i ++){
      NonBlockAsyncSocketClientWorker worker = new NonBlockAsyncSocketClientWorker();

      pool.execute(worker);
    }
  }

  public static void main(String[] args){
    NonBlockAsyncSocketClient client = new NonBlockAsyncSocketClient();

    try {
      client.init();
      client.start();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
