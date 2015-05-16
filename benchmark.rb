['/tpch/100/customer/customer.tbl', '/tpch/100/orders/orders.tbl', '/tpch/100/lineitem/lineitem.tbl'].each do |s|
    ((1..10).map do |i|
		`for i in \`seq 11 16\`;do ssh scyper$i "/usr/local/bin/flush_fs_caches"; done`
        print (`./build/hdfs_benchmark -n scyper11 -s /var/run/hdfs-sockets/dn -f #{s} -b #{1024**2} -t standard`).gsub("\n", '')+"\t"
    end)
    puts "\n"
end

['/tpch/100/customer/customer.tbl', '/tpch/100/orders/orders.tbl', '/tpch/100/lineitem/lineitem.tbl'].each do |s|
     ((1..10).map do |i|
         `for i in \`seq 11 16\`;do ssh scyper$i "/usr/local/bin/flush_fs_caches"; done`
         print (`./build/hdfs_benchmark -n scyper11 -s /var/run/hdfs-sockets/dn -f #{s} -b #{1024**2} -t scr`).gsub("\n", '')+"\t"
     end)
     puts "\n"
 end

['/tpch/100/customer/customer.tbl', '/tpch/100/orders/orders.tbl', '/tpch/100/lineitem/lineitem.tbl'].each do |s|
        ((1..10).map do |i|
            `for i in \`seq 11 16\`;do ssh scyper$i "/usr/local/bin/flush_fs_caches"; done`
            print (`./build/hdfs_benchmark -n scyper11 -s /var/run/hdfs-sockets/dn -f #{s} -b #{1024**2} -t zcr`).gsub("\n", '')+"\t"
        end)
  		puts "\n"
end
