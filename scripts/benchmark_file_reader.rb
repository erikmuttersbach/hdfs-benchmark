# Determine best buffer size
=begin
[:hot, :cold]. each do |h|
['', '--advise-sequential', '--advise-willneed', '--use-readahead', '--use-ioprio'].each do |opt|
puts "#{h}, #{opt}"
[1024, 4096, 1024*64, 1024*1024, 1024*1024*8, 1024*1024*64, 512*1024*1024].each do |buffer_size|
    (1..5).each do |i|
        `sudo drop_caches` if h == :cold
        print (`./build/file_reader -t file_read -f /home/erik/Downloads/2000M -b #{buffer_size} #{opt}`).gsub("\n", '').gsub(',', '.')+" "
    end
    puts " "
end
puts "\n"
end
end

[:hot, :cold]. each do |h|
['', '--advise-sequential', '--advise-willneed', '--use-readahead', '--use-ioprio'].each do |opt|
puts "#{h}, #{opt}"
[1024, 4096, 1024*64, 1024*1024, 1024*1024*8, 1024*1024*64, 512*1024*1024].each do |buffer_size|
    (1..5).each do |i|
        `sudo drop_caches` if h == :cold
        print (`./build/file_reader -t file_mmap -f /home/erik/Downloads/2000M -b #{buffer_size} #{opt}`).gsub("\n", '').gsub(',', '.')+" "
    end
    puts " "
end
puts "\n"
end
end
=end

[:hot, :cold]. each do |h|
puts "#{h}"
['1000M', '2000M', '5000M', '10000M', '15000M'].each do |file|
    (1..5).each do |i|
        `sudo drop_caches` if h == :cold
        print (`./build/file_reader -t file_read -f /home/erik/Downloads/#{file} -b #{(64*1024)}`).gsub("\n", '').gsub(',', '.')+" "
    end
    puts " "
end
puts "\n"
end

