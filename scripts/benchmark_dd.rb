=begin
[:cold]. each do |h|
['2000M'].each do |s|
[1024, 4096, 1024*64, 1024*1024, 1024*1024*8, 1024*1024*64, 512*1024*1024].each do |buffer_size|
    (1..5).map do |i|
        `sudo drop_caches` if h == :cold
        r = `dd if=/home/erik/Downloads/#{s} of=/dev/zero bs=#{buffer_size} 2>&1 | grep -i -o -E "[0-9]+(,[0-9]+){0,1} (MB|GB)/s"`
        v = r.gsub(',', '.').to_f * (r.include?("GB") ? 1000 : 1)
        print "#{v} "
    end
    print "\n"
end
end
end
=end

[:hot, :cold]. each do |h|
['1000M', '2000M', '5000M', '10000M', '15000M'].each do |s|
    (1..5).map do |i|
        `sudo drop_caches` if h == :cold
        r = `dd if=/home/erik/Downloads/#{s} of=/dev/zero bs=#{64*1024} 2>&1 | grep -i -o -E "[0-9]+(,[0-9]+){0,1} (MB|GB)/s"`
        v = r.gsub(',', '.').to_f * (r.include?("GB") ? 1000 : 1)
        print "#{v} "
    end
    print "\n"
end
end