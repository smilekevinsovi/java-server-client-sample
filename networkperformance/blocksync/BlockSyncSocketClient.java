package networkperformance.blocksync;

import networkperformance.DataGen;
import networkperformance.ServerClient;

import javax.xml.crypto.Data;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by julyhou on 15/12/23.
 */

class BlockSyncSocketClientWorker implements BlockSyncSocketClient.Worker {

  @Override
  public void run() {
    try {
      SocketChannel sc = SocketChannel.open();
      sc.connect(new InetSocketAddress(DataGen.masterHost, DataGen.masterPort));

      ByteBuffer buffer = DataGen.genByte(DataGen.SendPerMessage);
      int count = DataGen.TotalDataSize / DataGen.SendPerMessage ;

      System.out.printf("send %dB per message with count %d\n", DataGen.SendPerMessage, count);

      for(int i = 0; i < count; i ++){
        sc.write(buffer);
        buffer.flip();
      }

      sc.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}

public class BlockSyncSocketClient implements ServerClient {
  private ExecutorService pool;

  public interface Worker extends Runnable{}

  public void init() throws IOException {
    pool = Executors.newFixedThreadPool(DataGen.ClientThreadCount);
  }

  public void start() {
    for(int i = 0; i < DataGen.ClientThreadCount; i ++){
      BlockSyncSocketClientWorker worker = new BlockSyncSocketClientWorker();

      pool.execute(worker);
    }
  }

  public static void main(String[] args){
    BlockSyncSocketClient client = new BlockSyncSocketClient();
    try {
      client.init();
      client.start();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
