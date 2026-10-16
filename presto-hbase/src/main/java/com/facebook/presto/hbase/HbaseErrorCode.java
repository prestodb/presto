package com.facebook.presto.hbase;

import static com.facebook.presto.common.ErrorType.EXTERNAL;

import com.facebook.presto.common.ErrorCode;
import com.facebook.presto.common.ErrorType;
import com.facebook.presto.spi.ErrorCodeSupplier;


/**
 * 
 * @author spancer.ray
 *
 */
public enum HbaseErrorCode implements ErrorCodeSupplier {
  // Thrown when an Hbase error is caught that we were not expecting,
  // such as when a create table operation fails (even though we know it will succeed due to our
  // validation steps)
  UNEXPECTED_HBASE_ERROR(1, EXTERNAL),

  // Thrown when a ZooKeeper error is caught due to a failed operation
  ZOOKEEPER_ERROR(2, EXTERNAL),

  // Thrown when a serialization error occurs when reading/writing data from/to Hbase
  IO_ERROR(3, EXTERNAL),

  // Thrown when a table that is expected to exist does not exist
  HBASE_TABLE_DNE(4, EXTERNAL),

  HBASE_TABLE_CLOSE_ERR(5, EXTERNAL),

  HBASE_TABLE_EXISTS(6, EXTERNAL);

  private final ErrorCode errorCode;

  HbaseErrorCode(int code, ErrorType type) {
    errorCode = new ErrorCode(code + 0x0103_0000, name(), type);
  }

  @Override
  public ErrorCode toErrorCode() {
    return errorCode;
  }
}
