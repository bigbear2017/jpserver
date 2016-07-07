package com.skywalker.jpserver;

/**
 * @author caonn@mediav.com
 * @version 16/7/5.
 */
public class Range {
  public final long begin;
  public final long end;

  public Range() {
    this.begin = 0;
    this.end = 0;
  }

  public Range(long begin, long end) {
    this.begin = begin;
    this.end = end;
  }
  public long size() {
    return end - begin;
  }
}
