package networkperformance;

import java.io.IOException;

/**
 * Created by julyhou on 15/12/23.
 */
public interface ServerClient {
  public void init() throws IOException;
  public void start();
}
