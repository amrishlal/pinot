package org.apache.pinot.common.config.provider;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.Weigher;
import java.lang.ref.WeakReference;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


public class BrokerQueryResultCache implements QueryResultCache<PinotQuery, ResultTable> {

  /** Broker Instance TTL (in seconds) */
  private static final int TTL_BROKER = 30;

  /** Maximum size of cache in bytes */
  private static final int MAX_CACHE_SIZE_BYTES = 10 * 1024;

  private static final long DEFAULT_TIMEOUT_MS = 1000;

  /** Actual cache data structure */
  private Cache<PinotQuery, MemoryCacheItem> _cache;

  /** Keeps track of all cache keys for a particular table name. */
  private LoadingCache<String, ConcurrentLinkedQueue<WeakReference<PinotQuery>>> _tableKeys;
  /** Random number generator */
  private Random _random;

  private long _timeout;

  /**
   * Cache value holder that allows for keeping track of object expiry time.
   * Expiry Time = Current Time + Table TTL
   */
  public static class MemoryCacheItem implements CacheItem<PinotQuery, ResultTable> {
    private ResultTable _value;
    private long _expiryTime;

    private int _size;
    private Location _location;

    public MemoryCacheItem(ResultTable value, int tableTtl, int size) {
      _value = value;
      _expiryTime = System.currentTimeMillis() + tableTtl;
      _location = Location.MEMORY;
      _size = size + 16;
    }

    @Override
    public PinotQuery getKey() {
      return null;
    }

    @Override
    public ResultTable getValue() {
      return _expiryTime < System.currentTimeMillis() ? _value : null;
    }

    @Override
    public void save()
        throws QueryResultCacheException {
      // No special actions needed to save cached value to memory.
      return;
    }

    @Override
    public void delete()
        throws QueryResultCacheException {
      // No special actions needed to delete cached value from memory.
      return;
    }

    @Override
    public long getExpiryTime() {
      return _expiryTime;
    }

    @Override
    public int getSizeInBytes() {
      return _size;
    }
  }

  public BrokerQueryResultCache() {
    // Create new cache. All items in cache should expire based on TTL_BROKER (this is independent of per table TTL
    // that a user would specify. Note that Broker level TTL (which would be set by system administrator) overrides
    // Table level TTL set by the user. Items in cache will be evicted as per LRU policy as cache size approaches the
    // maximum allowable cache size limit of MAX_CACHE_SIZE_BYTES
    _cache =
        CacheBuilder.newBuilder().expireAfterWrite(TTL_BROKER, TimeUnit.SECONDS).maximumWeight(MAX_CACHE_SIZE_BYTES)
            .weigher(new Weigher<PinotQuery, MemoryCacheItem>() {
              @Override
              public int weigh(PinotQuery key, MemoryCacheItem value) {
                // Return size of the object being put into the cache.
                return value.getSizeInBytes();
              }
            }).build();

    // Keep track of all the cached queries for a particular table. This is being done to allow for invalidating or
    // getting information on all the cached queries for a particular table. For example, a user can invalidate all
    // cache entries for a particular table through the REST API.
    _tableKeys =
        CacheBuilder.newBuilder().build(new CacheLoader<String, ConcurrentLinkedQueue<WeakReference<PinotQuery>>>() {
          @Override
          public ConcurrentLinkedQueue<WeakReference<PinotQuery>> load(String key)
              throws Exception {
            // Do not throw any checked exceptions in this method.
            return new ConcurrentLinkedQueue<>();
          }
        });

    // Random number generator to randomly pick a few rows to get an estimate of cached data size.
    _random = new Random();
    _timeout = DEFAULT_TIMEOUT_MS;
  }

  @Override
  public void setTimeout(long timeout) {
    _timeout = timeout;
  }

  @Override
  public long getTimeout() {
    return _timeout;
  }

  /** Put {@link PinotQuery} and its associated {@link ResultTable} into the cache asynchronously. */
  public CompletableFuture<Void> put(PinotQuery pinotQuery, ResultTable resultTable) {
    System.out.println("\tAdding new entry into cache asynchronously.");
    CompletableFuture<Void> completableFuture = CompletableFuture.runAsync(new Runnable() {
      @Override
      public void run() {
        putSync(pinotQuery, resultTable);
      }
    }).completeOnTimeout(null, _timeout, TimeUnit.MILLISECONDS);

    return completableFuture;
  }

  private void putSync(PinotQuery pinotQuery, ResultTable resultTable) {
    // Copy input pinotQuery object for caching and remove its "timeoutMs" field.
    PinotQuery copiedPinotQuery = pinotQuery.deepCopy();
    copiedPinotQuery.queryOptions.remove("timeoutMs");

    System.out.println("\tCache entry size: " + getCachedSize(pinotQuery, resultTable));
    _cache.put(copiedPinotQuery, new MemoryCacheItem(resultTable, 30, getCachedSize(pinotQuery, resultTable)));

    // Update the tableName->keys map as well. No need for locking and synchronization. It's ok for _tableKeys
    // map to not be exactly in sync with _cache.
    String rawTableName = TableNameBuilder.extractRawTableName(pinotQuery.dataSource.getTableName());
    ConcurrentLinkedQueue<WeakReference<PinotQuery>> keys = _tableKeys.getUnchecked(rawTableName);
    keys.removeIf(k -> (k.get() == null));
    keys.add(new WeakReference<>(copiedPinotQuery));
  }

  public ResultTable get(PinotQuery pinotQuery) {
    // Remove "timeoutMs" from incoming query.
    String timeoutMs = pinotQuery.queryOptions.get("timeoutMs");
    pinotQuery.queryOptions.remove("timeoutMs");
    try {
      MemoryCacheItem item = _cache.getIfPresent(pinotQuery);
      if (item == null) {
        // Return null since query results are not cached.
        System.out.println("\tCache entry not found.");
        return null;
      }

      ResultTable result = item.getValue();
      if (result == null) {
        // Invalidate cache entry and return null since cache entry has outlived its TTL. Note that we are
        // checking table level TTL here. Cache entries will automatically be invalidated if Broker TTL is
        // reached since Broker level TTL is specified during _cache construction.
        _cache.invalidate(pinotQuery);
        System.out.println("\tCache entry expired.");
        return null;
      }

      // Return cached results.
      System.out.println("\tCache entry found.");
      return result;
    } finally {
      // Put "timeoutMs" back into the incoming query.
      pinotQuery.queryOptions.put("timeoutMs", timeoutMs);
    }
  }

  @Override
  public CompletableFuture<Void> remove(PinotQuery pinotQuery) {
    return null;
  }

  @Override
  public CompletableFuture<Void> remove(String rawTableName) {
    CompletableFuture<Void> completableFuture = CompletableFuture.runAsync(new Runnable() {
      @Override
      public void run() {
        removeSync(rawTableName);
      }
    }).completeOnTimeout(null, _timeout, TimeUnit.MILLISECONDS);

    return completableFuture;
  }

  private void removeSync(String rawTableName) {
    ConcurrentLinkedQueue<WeakReference<PinotQuery>> keys = _tableKeys.getUnchecked(rawTableName);
    keys.removeIf(k -> (k.get() == null));
    _cache.invalidateAll(keys);
  }

  @Override
  public void reset() {
  }

  /** @return approximate size (in bytes) occupied by the input {@link ResultTable} */
  public int getCachedSize(PinotQuery pinotQuery, ResultTable table) {
    int totalSizeInBytes = 0;

    // Calculate memory upper bound of pinotQuery object.
    totalSizeInBytes += pinotQuery.toString().length() * 2;

    // Calculate memory upper bound of ResultTable.
    DataSchema dataSchema = table.getDataSchema();
    int numColumns = dataSchema.size();
    long tableSize = 0;
    for (int i = 0; i < 3; i++) {
      int rowIndex = _random.nextInt(table.getRows().size());
      Object[] row = table.getRows().get(rowIndex);
      long rowSize = 0;
      for (int j = 0; j < numColumns; j++) {
        switch (dataSchema.getColumnDataType(j)) {
          case INT:
          case FLOAT:
          case BOOLEAN:
            rowSize += 4;
            break;
          case LONG:
          case DOUBLE:
            rowSize += 8;
            break;
          case BYTES:
          case OBJECT:
          case STRING:
          case JSON:
          case TIMESTAMP:
          case INT_ARRAY:
          case LONG_ARRAY:
          case BYTES_ARRAY:
          case BIG_DECIMAL:
          case FLOAT_ARRAY:
          case DOUBLE_ARRAY:
          case STRING_ARRAY:
          case BOOLEAN_ARRAY:
          case TIMESTAMP_ARRAY:
            rowSize += row[j].toString().length() * 2;
            break;
          default:
            break;
        }
      }
      tableSize += rowSize;
    }

    totalSizeInBytes += (tableSize / 3) * table.getRows().size();
    return totalSizeInBytes;
  }
}
