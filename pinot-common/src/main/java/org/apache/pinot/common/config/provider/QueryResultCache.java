package org.apache.pinot.common.config.provider;

import java.util.concurrent.CompletableFuture;


/**
 * QueryResultCache stores key-value pairs in a cache. Differnt key-value pairs can be stored in different
 * storage locations (memory, disk, another Pinot Broker accessible over network). The decision on which storage
 * location is used for a particular key-value pair is implementation specific and would depend upon factors such as
 * amount of space need to store the key-value pair, amount of space available on the storage medium, the amount of time
 * it takes to recompute the key-value pair being cached, and whether the key-value pair needs to be shared between
 * different Brokers over the network.
 */
public interface QueryResultCache<K, V> /*extends TableConfigChangeListener, ClusterConfigChangeListener */ {
  enum Location {
    MEMORY,          // Cached in local memory
    DISK,            // Cached on local DISK
    BROKER_AFFINITY  // Cache on another Pinot Broker.
  }

  /**
   * Internally the cache will store pairs of key K and value CacheItem(K, V) where K and V are actual key and value
   * are the actual user-supplied key value pairs that need to be stored in cache. Wrapping key-value within
   * CachteItem allows for maintaining TTL and customizing the storage location. For example, a DiskCacheItem would be
   * used to store cached item on disk and BrokerCacheItem would be used to store cached item on another Broker.
   */
  interface CacheItem<K, V> {
    K getKey();

    V getValue();

    /** Save cached key-value pair on the underlying storage medium (MEMORY, DISK, NETWORK_BROKER, etc) */
    void save()
        throws QueryResultCacheException;

    /**  Remove this ke-value pair from the underlying store where the key-value pair was saved. */
    void delete()
        throws QueryResultCacheException;

    /** How long before this cache item expires. */
    long getExpiryTime();

    /** Memory size of this {@link CacheItem} object. */
    int getSizeInBytes();
  }

  class QueryResultCacheException extends RuntimeException {
    public QueryResultCacheException(String message) {
      super(message);
    }

    public QueryResultCacheException(String message, Throwable t) {
      super(message, t);
    }
  }

  /** Time duration after which async cache operations will end. */
  void setTimeout(long timeout);

  /** @return Async operation timeout interval */
  long getTimeout();

  /** Put a key-value pair in the cache asynchronously. */
  CompletableFuture<Void> put(K query, V result);

  /** @return value associated with the input key. If the key doesn't exist in the cache or has expired, return null. */
  V get(K pinotQuery);

  /** Asynchronously remove specified key from the cache. */
  CompletableFuture<Void> remove(K pinotQuery);

  /** Asynchronously remove all the keys associated with this table. */
  CompletableFuture<Void> remove(String rawTableName);

  /** Remove all key-value pairs from the cache */
  void reset();
}
