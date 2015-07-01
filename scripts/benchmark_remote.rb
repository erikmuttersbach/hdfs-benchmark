def start_ifstat()
  `ifstat -i ib0 > ifstat.out &`
end

def stop_ifstat()
  sleep 1
  `killall ifstat`
  
  lines = File.read('ifstat.out').split("\n").drop(2)
  traffic_in = 0
  traffic_out = 0
  t = (lines.map do |line|
    s = line.split(/\s+/)
    traffic_in += s[0].to_f
    traffic_out += s[1].to_f
  end)
  
  return traffic_in/1024.0, traffic_out/1024.0
end


# Determine best buffer size
=begin
[:hot, :cold]. each do |h|
['/tpch/100/customer/customer.tbl', '/tpch/100/orders/orders.tbl'].each do |file|
puts "#{h}, #{file}"
[1024, 4096, 1024*64, 1024*1024, 1024*1024*8, 1024*1024*64, 512*1024*1024].each do |buffer_size|
    (1..5).each do |i|
        %x(for i in `seq 11 16`;do ssh scyper$i "/usr/local/bin/flush_fs_caches"; done;) if h == :cold
        
	start_ifstat
	v = (`env LD_LIBRARY_PATH=./build/lib ./build/hdfs_reader -n scyper11 -s /var/run/hdfs-sockets/dn -f #{file} -b #{buffer_size} -t scr`).gsub("\n", '').gsub(',', '.')
    	t_in, t_out = stop_ifstat
	print "#{v} #{t_in} #{t_out} "
	
	end 
    puts " "
end
end
end
=end

# Show read speed for different files
[:hot, :cold]. each do |h|
['/tpch/100/customer/customer.tbl', '/tpch/100/orders/orders.tbl', '/tpch/100/lineitem/lineitem.tbl'].each do |file|
    (1..5).each do |i|
        %x(for i in `seq 11 16`;do ssh scyper$i "/usr/local/bin/flush_fs_caches"; done;) if h == :cold
        
        start_ifstat
        v = (`env LD_LIBRARY_PATH=./build/lib ./build/hdfs_reader -n scyper11 -s /var/run/hdfs-sockets/dn -f #{file} -b #{(1024*64)} -t scr`).gsub("\n", '').gsub(',', '.')
        t_in, t_out = stop_ifstat
        print "#{v} #{t_in} #{t_out} "
    end 
    puts " "
end
end

# Determine best block size
=begin
[:hot, :cold]. each do |h|
['2000M'].each do |file|
[1024*1024].each do |buffer_size|
[:standard, :scr, :zcr].each do |read_type|
[128, 256, 512].each do |block_size|
    (1..5).each do |i|
        `sudo drop_caches` if h == :cold
        print (`./build/hdfs_reader -f /data/bs#{block_size}/#{file} -b #{buffer_size} -t #{read_type}`).gsub("\n", '').gsub(',', '.')+" "
    end
    puts " "
end
end
end
end
end

# Skip Checksums
=begin
[:hot, :cold]. each do |h|
(1..5).each do |i|
    `sudo drop_caches` if h == :cold
    print (`./build/hdfs_reader -f /data/bs512/2000M -b #{(1024*64)} -t scr`).gsub("\n", '').gsub(',', '.')+" "
end
puts " "
end
=end
