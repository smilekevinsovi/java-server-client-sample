package networkperformance.nonblocksync;

import networkperformance.DataGen;
import networkperformance.ServerClient;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by julyhou on 15/12/24.
 */

class NonBlockSyncSocketClientWorker implements NonBlockSyncSocketClient.Worker {

  @Override
  public void run() {
    try {
      Selector selector = Selector.open();
      SocketChannel sc = SocketChannel.open();
      sc.configureBlocking(false);
      sc.connect(new InetSocketAddress(DataGen.masterHost, DataGen.masterPort));

      ByteBuffer buffer = DataGen.genByte(DataGen.SendPerMessage);
      int count = DataGen.TotalDataSize / DataGen.SendPerMessage ;
      int times = 0;
      buffer.flip();

      System.out.printf("send %dB per message with count %d\n", DataGen.SendPerMessage, count);

      SelectionKey selectionKey = sc.register(selector, SelectionKey.OP_CONNECT);

      while(true) {
        int num = selector.select();

        if(num == 0){
          continue;
        }

        Iterator<SelectionKey> iter = selector.selectedKeys().iterator();

        while(iter.hasNext()){
          final SelectionKey key = iter.next();
          iter.remove();

          if(!key.isValid()){
            continue;
          }

          if(key.isConnectable()){
            SocketChannel scc = (SocketChannel)key.channel();

            if(!scc.finishConnect()){
              continue;
            }

            scc.register(selector, SelectionKey.OP_WRITE);
          }

          if(key.isWritable()){
            key.interestOps(0);
            SocketChannel scc = (SocketChannel)key.channel();

            while(true){
              int writeNum = sc.write(buffer);

              if(writeNum == 0){
                key.interestOps(SelectionKey.OP_WRITE);
                break;
              }

              //System.out.printf("%d %d\n", times, count);

              if (!buffer.hasRemaining()) {
                buffer.flip();
                times++;

                if (times == count) {
                  selector.close();
                  sc.close();
                  return;
                }
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

public class NonBlockSyncSocketClient implements ServerClient{
  private ExecutorService pool;

  public interface Worker extends Runnable{}

  public void init() throws IOException {
    pool = Executors.newFixedThreadPool(DataGen.ClientThreadCount);
  }

  public void start() {
    for(int i = 0; i < DataGen.ClientThreadCount; i ++){
      NonBlockSyncSocketClientWorker worker = new NonBlockSyncSocketClientWorker();

      pool.execute(worker);
    }
  }

  public static void main(String[] args){
    NonBlockSyncSocketClient client = new NonBlockSyncSocketClient();
    try {
      client.init();
      client.start();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
