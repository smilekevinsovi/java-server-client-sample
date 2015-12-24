package networkperformance;

import networkperformance.blocksync.BlockSyncSocketClient;
import networkperformance.blocksync.BlockSyncSocketServer;
import networkperformance.nonblockasync.NonBlockAsyncSocketClient;
import networkperformance.nonblockasync.NonBlockAsyncSocketServer;

import java.io.IOException;

/**
 * Created by julyhou on 15/12/23.
 */
public class Main {

  public static ServerClient createServer(int type) {
    switch (type){
      case 1 :
      case 2 :
        return new NonBlockAsyncSocketServer();

      case 0 :
      default:
        return new BlockSyncSocketServer();
    }
  }

  public static ServerClient createClient(int type) {
    switch (type){
      case 1 :
      case 2 :
        return new NonBlockAsyncSocketClient();

      case 0 :
      default:
        return new BlockSyncSocketClient();
    }
  }


  public static void main(String[] args){
    if(args.length < 8){
      System.out.println("should contains 6 paramtesr, 0/1/2, block/non-block/aync-non-block" +
          "1/0 for master/slave, total-data, mthread, cthread, size-per " +
          "master-host-name, master-port");
      return;
    }

    int type = Integer.parseInt(args[0]);
    int role = Integer.parseInt(args[1]);

    DataGen.TotalDataSize = Integer.parseInt(args[2]) * 1024;
    DataGen.MasterThreadCount = Integer.parseInt(args[3]);
    DataGen.ClientThreadCount = Integer.parseInt(args[4]);
    DataGen.SendPerMessage = Integer.parseInt(args[5]);
    DataGen.masterHost = args[6];
    DataGen.masterPort = Integer.parseInt(args[7]);

    ServerClient serverClient = null;
    if(role == 0){
      serverClient = createServer(type);
    }else{
     serverClient = createClient(type);
    }

    try {
      serverClient.init();
      serverClient.start();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
