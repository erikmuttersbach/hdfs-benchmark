['1000M', '2000M', '5000M', '10000M', '15000M'].each do |s|
    (1..5).map do |i|
        #`sudo drop_caches`
        r = `dd if=/home/erik/Downloads/#{s} of=/dev/zero 2>&1 | grep -i -o -E "[0-9]+(,[0-9]+){0,1} (MB|GB)/s"`
        v = r.gsub(',', '.').to_f * (r.include?("GB") ? 1000 : 1)
        print "#{v} "
    end
    print "\n"
end
