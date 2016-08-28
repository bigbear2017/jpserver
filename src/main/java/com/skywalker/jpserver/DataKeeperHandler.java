package com.skywalker.jpserver;

import com.google.common.collect.Lists;
import com.skywalker.zkmgr.NodeInfo;
import com.skywalker.zkmgr.ZKInfo;
import com.skywalker.zkmgr.ZKManager;
import gnu.trove.map.TLongIntMap;
import gnu.trove.map.hash.TLongIntHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.skywalker.jpserver.Constants.*;


/**
 * This handler will handle data internally. All push requests will be cached first.
 * When the cache has been synchronized, the cache will be added to data and cleared.
 * There are some designs here. Because we need to write to the cache all the time,
 * the data map need to read the cache when the cache is full or is old, however we
 * can not write and read at the same time, we need a lock to keep read and write correct.
 * To avoid lock, we use a cache list. When a cache is full, it will not be written anymore.
 * Only when it has been processed, it will be available.
 *
 * @author caonn@mediav.com
 * @version 16/7/13.
 */
public class DataKeeperHandler implements DataKeeperService.Iface {
  private static Logger LOGGER = LoggerFactory.getLogger(DataKeeperHandler.class);
  TLongIntMap dataMap = new TLongIntHashMap(INITIAL_CAPACITY, LOAD_FACTOR, NO_ENTRY_KEY, NO_ENTRY_VALUE);
  List<TLongIntMap> cacheList = Lists.newArrayList();
  AtomicIntegerArray lockArray = new AtomicIntegerArray(2);
  BlockingQueue<List<Point>> dataPointQueue;
  Lock lock = new ReentrantReadWriteLock().writeLock();
  int queueWaitTime = 1000;
  private long updateCounter = 0;
  private long updateTime = System.currentTimeMillis();
  private int updateThresh = 100;
  private int timeThresh = 2000;
  private int buffSize = 2;
  private int maxCacheSize = 1000;

  public DataKeeperHandler( int dataPointQueueSize, int queueWaitTime, int updateThresh, int timeThresh ) {
    this.dataPointQueue = new LinkedBlockingQueue<>(dataPointQueueSize);
    this.queueWaitTime = queueWaitTime;
    this.updateThresh = updateThresh;
    this.timeThresh = timeThresh;
    for( int i = 0; i < 2; i++) {
      cacheList.add(new TLongIntHashMap(INITIAL_CAPACITY, LOAD_FACTOR, NO_ENTRY_KEY, NO_ENTRY_VALUE));
      lockArray.set(i, 0);
    }

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

  public void putToCache() {
    try {
      List<Point> dataList = dataPointQueue.poll(queueWaitTime, TimeUnit.MILLISECONDS);
      for( int i = 0; i < buffSize; i++ ) {
        boolean available = lockArray.compareAndSet(i, 0, 1);
        if( available ) {
          TLongIntMap cache = cacheList.get(i);
          dataList.stream().forEach(p -> cache.put(p.getIndex(), p.getValue()));
          updateCounter++;
          if( updateCounter >= maxCacheSize ) {
            updateCounter = 0;
            updateTime = System.currentTimeMillis();
            lockArray.set(i, 1);
          }
        }
      }

    } catch (InterruptedException e) {
      LOGGER.error("Can not poll from queue, exception: {}", e);
    }
  }

  /**
   * Send all data to the primary server to synchronize all data.
   */
  public boolean updateCache(boolean local, ZKInfo zkInfo) {
    long timeDiff = System.currentTimeMillis() - updateTime;
    if ( updateCounter < updateThresh &&  timeDiff < timeThresh ) {
      return false;
    }
    if( local ) {
      TLongIntMap cache = new TLongIntHashMap();
      cache.forEachEntry( (l, i) -> { int value = dataMap.get(l) + i; dataMap.put(l, value); return true;} );
    } else {
      try {
        NodeInfo nodeInfo = ZKManager.getPrimaryServer(zkInfo);

        //TODO sync data to primary server.
      } catch (Exception e) {
      }
    }
    updateTime = System.currentTimeMillis();
    updateCounter = 0;
    return true;
  }

  public long getUpdateCounter() {
    return updateCounter;
  }

  public long getUpdateTime() {
    return updateTime;
  }
}
