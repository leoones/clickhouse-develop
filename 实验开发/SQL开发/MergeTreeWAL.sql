 背景: 多个客户端小批量频繁写入.就会引发DB: Exception: Too parts错误. Write-Ahead Log 会先把数据写到内存，等“攒”到一定程度后再回写到磁盘.
      默认已开启.
 
 相关参数: min_rows_for_compact_part:
          min_bytes_for_wide_part:
          min_rows_for_wide_part: 
          write_ahead_log_max_bytes: 限制wal.bin的大小，默认值为1G
          
