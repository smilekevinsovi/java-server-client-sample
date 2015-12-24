package networkperformance.blocksync;

import networkperformance.DataGen;
import networkperformance.ServerClient;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by julyhou on 15/12/23.
 */
class BlockSyncSocketServerWorker implements BlockSyncSocketServer.ServerWorker {
  private SocketChannel sc;
  public BlockSyncSocketServerWorker(SocketChannel sc){
    this.sc = sc;
  }

  @Override
  public void run() {
    long start = System.currentTimeMillis();
    ByteBuffer buffer = ByteBuffer.allocate(DataGen.SendPerMessage);
    int count = 0;
    try {
      while(sc.read(buffer) != -1){
        buffer.flip();
        count ++;
      }

      sc.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    long end = System.currentTimeMillis();

    System.out.printf("%d %d %d %d\n", start, end, end - start, count);
  }
}

public class BlockSyncSocketServer implements ServerClient {
  private ServerSocketChannel serverSC = null;
  private ExecutorService pool;

  public interface ServerWorker extends Runnable {}

  public ServerWorker createWorker(SocketChannel sc) {
    return new BlockSyncSocketServerWorker(sc);
  }

  public void init() throws IOException{
    serverSC = ServerSocketChannel.open();
    serverSC.bind(new InetSocketAddress(DataGen.masterPort));
    pool = Executors.newFixedThreadPool(DataGen.MasterThreadCount);
  }

  public void start(){
    SocketChannel sc = null;
    while(true){
      try {
        sc = serverSC.accept();
        ServerWorker worker = createWorker(sc);
        pool.execute(worker);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  public static void main(String[] args){
    BlockSyncSocketServer server = new BlockSyncSocketServer();

    try {
      server.init();
      server.start();
    } catch (IOException e) {
      e.printStackTrace();
    }

  }

}
