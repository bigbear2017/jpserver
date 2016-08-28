package com.skywalker.jpserver;

import org.kohsuke.args4j.CmdLineParser;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

/**
 * There are two modes to use, locally or distributively. The distributive mode rely on zookeeper.
 * <P>
 *
 * When it runs locally, the local machine will become the server. All data can be pushed
 * to the local server. Since there is only one server, the QPS will not be large.
 *
 * When it runs in parallel,
 * First, all servers will register itself to zookeeper
 * Second, all servers will receive data from clients and put the data in cache.
 * Third, every a few seconds, the local server will push cache to the center server.
 * Fourth, when all local servers finished, the center server broadcast data to all local server.
 * @author caonn@mediav.com
 * @version 16/7/14.
 */
public class DataKeeperServer {

  public void initialize(Properties properties) {
  }
  public static void main(String [] args) throws Exception {
    ServerOptions options = new ServerOptions();
    CmdLineParser parser = new CmdLineParser(new ServerOptions());
    parser.parseArgument(args);

    Properties properties = new Properties();
    properties.load(new FileInputStream(new File(options.getPropFile())));

  }
}
