#['1000M', '2000M', '5000M', '10000M', '15000M'].each do |s|
['1000M', '5000M'].each do |s|
#['2000M'].each do |s|
#[1024, 4096, 1024*64, 1024*1024, 1024*1024*8, 1024*1024*64, 512*1024*1024].each do |s|
    ((1..10).map do |i|
	#`sudo drop_caches`
        print (`./build/hdfs_benchmark -f /tmp/#{s} -b #{1024**2} -t scr`).gsub("\n", '')+"\t"
    end)
    puts "\n"
end

