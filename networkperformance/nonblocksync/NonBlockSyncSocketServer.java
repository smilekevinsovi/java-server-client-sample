package networkperformance.nonblocksync;

import networkperformance.DataGen;
import networkperformance.ServerClient;

import javax.xml.crypto.Data;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class ServerWorkerAttachment {

}

class NonBlockSyncSocketServerWorker implements NonBlockSyncSocketServer.ServerWorker {
  private SocketChannel sc;
  public NonBlockSyncSocketServerWorker(SocketChannel sc){
    this.sc = sc;
  }

  @Override
  public void run() {
    try {
      Selector readSelector = Selector.open();
      sc.configureBlocking(false);
      sc.register(readSelector, SelectionKey.OP_READ);

      long start = System.currentTimeMillis();
      ByteBuffer buffer = ByteBuffer.allocate(DataGen.SendPerMessage);
      buffer.clear();
      int count = 0;

      int totalNum = 0;

      while(true){
        readSelector.select();
        Iterator<SelectionKey> iter = readSelector.selectedKeys().iterator();

        while(iter.hasNext()){
          SelectionKey key = iter.next();
          iter.remove();

          if(key.isValid() && key.isReadable()){
            while(true) {
              int num = sc.read(buffer);

              if(num == -1){
                sc.close();
                readSelector.close();
                return;
              }

              // no more data
              if(num == 0){
                break;
              }

              totalNum += num;

              count ++;
              buffer.clear();

              if(totalNum == DataGen.TotalDataSize){
                long end = System.currentTimeMillis();
                System.out.printf("%d %d %d %d\n", start, end, end - start, count);
              }
            }
          }
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}

public class NonBlockSyncSocketServer implements ServerClient{
  private ServerSocketChannel serverSC = null;
  private ExecutorService pool;

  private Selector selector;

  public interface ServerWorker extends Runnable {}

  public ServerWorker createWorker(SocketChannel sc) {
    return new NonBlockSyncSocketServerWorker(sc);
  }

  public void init() throws IOException{
    serverSC = ServerSocketChannel.open();
    serverSC.bind(new InetSocketAddress(DataGen.masterPort));
    serverSC.configureBlocking(false);
    pool = Executors.newFixedThreadPool(DataGen.MasterThreadCount);
    selector = Selector.open();

    serverSC.register(selector, SelectionKey.OP_ACCEPT);
  }

  public void start(){

    while(true){
      try {
        selector.select();

        int num = selector.selectedKeys().size();

        if(num == 0){
          continue;
        }

        Iterator<SelectionKey> iter = selector.selectedKeys().iterator();

        while(iter.hasNext()){
          SelectionKey key = iter.next();
          iter.remove();

          if(!key.isValid() || !key.isAcceptable()){
            continue;
          }

          SocketChannel sc = serverSC.accept();

          ServerWorker worker = createWorker(sc);
          pool.execute(worker);
        }

      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  public static void main(String[] args){
    NonBlockSyncSocketServer server = new NonBlockSyncSocketServer();

    try {
      server.init();
      server.start();
    } catch (IOException e) {
      e.printStackTrace();
    }

  }
}
