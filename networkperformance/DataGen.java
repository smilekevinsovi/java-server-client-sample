package networkperformance;

import java.nio.ByteBuffer;

/**
 * Created by julyhou on 15/12/23.
 */
public class DataGen {
  public static int TotalDataSize = 1024 * 1024 * 24;
  public static int MasterThreadCount = 128;
  public static int ClientThreadCount = 128;
  public static int SendPerMessage = 2048; // B
  public static String masterHost = "localhost";
  public static int masterPort = 9999;

  // size in Byte
  public static ByteBuffer genByte(int size){
    ByteBuffer buffer = ByteBuffer.allocate(size);
    int intLength = size / 4;

    for(int i = 0; i < intLength; i ++){
      buffer.putInt(i % 4 + 1024);
    }

    for(int i = intLength * 4; i < size; i ++){
      buffer.putChar('A');
    }

    return buffer;
  }

  // size in KB
  public static ByteBuffer genKB(int size){
    ByteBuffer buffer = ByteBuffer.allocate(size * 1024);
    ByteBuffer kBuffer = genByte(1024);

    for(int i = 0; i < size; i ++){
      buffer.put(kBuffer.array());
    }

    return buffer;
  }

  // size in MB
  public static ByteBuffer genMB(int size){
    ByteBuffer buffer = ByteBuffer.allocate(size * 1024 * 1024);
    ByteBuffer mBuffer = genKB(1024);

    for(int i = 0; i < size; i ++){
      buffer.put(mBuffer.array());
    }

    return buffer;
  }
}
