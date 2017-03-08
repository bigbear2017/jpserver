package com.skywalker.jpserver;

import org.kohsuke.args4j.Option;

/**
 * JP server options.
 * @author caonn@mediav.com
 * @version 16/8/22.
 */
public class ServerOptions {
  @Option(name = "-props", usage = "property file for server")
  String propFile;

  public String getPropFile() {
    return propFile;
  }
}
