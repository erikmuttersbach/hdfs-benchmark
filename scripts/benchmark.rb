# Determine best buffer size
[:hot]. each do |h|
[512].each do |block_size|
['2000M'].each do |file|
[:standard, :scr, :zcr].each do |read_type|
#puts "#{h}, BS #{block_size}, #{file}, #{read_type}"
[1024, 4096, 1024*64, 1024*1024, 1024*1024*8, 1024*1024*64, 512*1024*1024].each do |buffer_size|
    (1..5).each do |i|
        `sudo drop_caches` if h == :cold
        print (`./build/hdfs_reader -f /data/bs#{block_size}/#{file} -b #{buffer_size} -t #{read_type}`).gsub("\n", '').gsub(',', '.')+" "
    end
    puts " "
end
#puts "\n"
end
end
end
end

# Determine best block size
=begin
[:hot, :cold]. each do |h|
['2000M'].each do |file|
[64*1024].each do |buffer_size|
[:standard, :scr].each do |read_type|
puts "#{h}, #{buffer_size} buffer, #{file}, #{read_type}"
[128, 256, 512].each do |block_size|
    (1..5).each do |i|
        `sudo drop_caches` if h == :cold
        print (`./build/hdfs_reader -f /data/bs#{block_size}/#{file} -b #{buffer_size} -t #{read_type}`).gsub("\n", '').gsub(',', '.')+" "
    end
    puts " "
end
puts "\n"
end
end
end
end
=end

# Show read speed for different files
=begin
[:hot, :cold]. each do |h|
[1024*64].each do |buffer_size|
[512].each do |block_size|
[:scr].each do |read_type|
puts "#{h}, #{buffer_size} buffer, #{read_type}, VS #{block_size}"
['1000M', '2000M', '5000M', '10000M', '15000M'].each do |file|
    (1..5).each do |i|
        `sudo drop_caches` if h == :cold
        print (`./build/hdfs_reader -f /data/bs#{block_size}/#{file} -b #{buffer_size} -t #{read_type}`).gsub("\n", '').gsub(',', '.')+" "
    end
    puts " "
end
puts "\n"
end
end
end
end
=end

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
