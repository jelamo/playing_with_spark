package com.reevoo.snowplow

import org.apache.hadoop.fs.s3native.NativeS3FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.PathFilter
import org.apache.hadoop.fs.FileStatus

class CustomS3FileSystem extends NativeS3FileSystem {

  override def globStatus(pathPattern: Path, filter: PathFilter): Array[FileStatus] = {
    super.listStatus(pathPattern).filter(fileStatus => filter.accept(fileStatus.getPath))
  }

}

