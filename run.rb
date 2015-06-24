21.downto(1).each do |thread_count| 
  (1..5).each do |i|
    #%x(for i in `seq 11 16`;do ssh scyper$i "/usr/local/bin/flush_fs_caches"; done)
    r = %x(build/src/q14 #{thread_count} scyper11 /user/hive/warehouse/tpch_parquet.db/lineitem /user/hive/warehouse/tpch_parquet.db/part | grep duration)
    print r.gsub(/[^0-9]/, '')+" "
  end
  print "\n"
end
