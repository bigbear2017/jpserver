package com.skywalker.jpserver;

import com.google.common.collect.Lists;
import gnu.trove.map.TLongIntMap;
import gnu.trove.map.hash.TLongIntHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.skywalker.jpserver.Constants.*;
/**
 * This handler will handle data internally. All push requests will be cached first.
 * When the cache has been synchronized, the cache will be added to data and cleared.
 * @author caonn@mediav.com
 * @version 16/7/13.
 */
public class DataKeeperHandler implements DataKeeperService.Iface, Runnable {
  private static Logger LOGGER = LoggerFactory.getLogger(DataKeeperHandler.class);
  TLongIntMap dataMap = new TLongIntHashMap(INITIAL_CAPACITY, LOAD_FACTOR, NO_ENTRY_KEY, NO_ENTRY_VALUE);
  TLongIntMap cache = new TLongIntHashMap(INITIAL_CAPACITY, LOAD_FACTOR, NO_ENTRY_KEY, NO_ENTRY_VALUE);
  BlockingQueue<List<Point>> dataPointQueue;
  int queueWaitTime = 1000;
  private long updateCounter = 0;
  private long updateTime = System.currentTimeMillis();

  public DataKeeperHandler( int dataPointQueueSize, int queueWaitTime ) {
    this.dataPointQueue = new LinkedBlockingQueue<>(dataPointQueueSize);
    this.queueWaitTime = queueWaitTime;
  }

  /**
   * Clients push the data list to local server. Local server will sync data to primary server.
   *
   * @param dataList the data from client
   * @return state of the push
   * @throws org.apache.thrift.TException
   */
  public boolean push(List<Point> dataList) throws org.apache.thrift.TException {
    try {
      dataPointQueue.put(dataList);
    } catch ( InterruptedException e ) {
      LOGGER.error("Can not put data list, exception : {}", e);
      return false;
    }
    return true;
  }

  public List<Point> pull() throws org.apache.thrift.TException {
    final List<Point> points = Lists.newArrayList();
    dataMap.forEachEntry((i, l) -> points.add(new Point().setIndex(i).setValue(l)));
    return points;
  }

  @Override
  public void run() {
    try {
      List<Point> dataList = dataPointQueue.poll(queueWaitTime, TimeUnit.MILLISECONDS);
      dataList.stream().forEach(p -> cache.put(p.getIndex(), p.getValue()));
      updateCounter++;
    } catch (InterruptedException e) {
      LOGGER.error("Can not poll from queue, exception: {}", e);
    }
  }

  /**
   * Send all data to the primary server to synchronize all data.
   */
  public void pushCache() {
    updateTime = System.currentTimeMillis();
    updateCounter = 0;
  }

  public long getUpdateCounter() {
    return updateCounter;
  }

  public long getUpdateTime() {
    return updateTime;
  }
}
