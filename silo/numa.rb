#!/usr/bin/env ruby

require 'fileutils'

numa_h = '
available: 8 nodes (0-7)
node 0 cpus: 0 1 2 3 7 8 9 14 15 16 17 21 22 23 112 113 114 115 119 120 121 126 127 128 129 133 134 135
node 0 size: 63939 MB
node 0 free: 54296 MB
node 1 cpus: 4 5 6 10 11 12 13 18 19 20 24 25 26 27 116 117 118 122 123 124 125 130 131 132 136 137 138 139
node 1 size: 64506 MB
node 1 free: 48734 MB
node 2 cpus: 28 29 30 31 35 36 37 42 43 44 45 49 50 51 140 141 142 143 147 148 149 154 155 156 157 161 162 163
node 2 size: 64506 MB
node 2 free: 54324 MB
node 3 cpus: 32 33 34 38 39 40 41 46 47 48 52 53 54 55 144 145 146 150 151 152 153 158 159 160 164 165 166 167
node 3 size: 64506 MB
node 3 free: 62090 MB
node 4 cpus: 56 57 58 59 63 64 65 70 71 72 73 77 78 79 168 169 170 171 175 176 177 182 183 184 185 189 190 191
node 4 size: 64506 MB
node 4 free: 62714 MB
node 5 cpus: 60 61 62 66 67 68 69 74 75 76 80 81 82 83 172 173 174 178 179 180 181 186 187 188 192 193 194 195
node 5 size: 64506 MB
node 5 free: 63280 MB
node 6 cpus: 84 85 86 87 91 92 93 98 99 100 101 105 106 107 196 197 198 199 203 204 205 210 211 212 213 217 218 219
node 6 size: 64506 MB
node 6 free: 63635 MB
node 7 cpus: 88 89 90 94 95 96 97 102 103 104 108 109 110 111 200 201 202 206 207 208 209 214 215 216 220 221 222 223
node 7 size: 64483 MB
node 7 free: 63672 MB
node distances:
node   0   1   2   3   4   5   6   7
  0:  10  11  21  21  21  21  21  21
  1:  11  10  21  21  21  21  21  21
  2:  21  21  10  11  21  21  21  21
  3:  21  21  11  10  21  21  21  21
  4:  21  21  21  21  10  11  21  21
  5:  21  21  21  21  11  10  21  21
  6:  21  21  21  21  21  21  10  11
  7:  21  21  21  21  21  21  11  10
'
numa_h = `numactl -H`
if ARGV.size < 2
  puts "usage: ruby "+File.basename(__FILE__)+" nodes n_worker/node [n_logger/node] [n_logger/core]"
  exit
end

def link_dir(node_id, dir_id, log_id)
  top = "/mnt/pmem#{node_id}"
  src = "log#{log_id}"
  dst = "#{top}/#{ENV['USER']}/log#{dir_id}"
  if Dir.exist?(top)
    if !Dir.exist?(dst)
      Dir.mkdir(dst)
    end
    FileUtils.rm_f src
    FileUtils.ln_s dst, src
  end
end

case ARGV[0]
when /^\d+$/
  nodes = ARGV[0].to_i.times.to_a
when /,/
  nodes = ARGV[0].split(",").map{|x|x.to_i}
when /^(\d+)-(\d+)$/
  nodes = ($1.to_i..$2.to_i).to_a
end
n_worker = ARGV[1].to_i
n_logger = (ARGV[2]||1).to_i
n_logger_core = (ARGV[3]||n_logger).to_i

if n_logger > n_worker
  raise "n_logger exceeds n_worker"
end

cpus = {}
numa_h.each_line do |l|
  if /^node (\d+) cpus: (.*)$/=~l
    node = $1.to_i
    cpus[node] = a = $2.split
    if a.size < n_worker
      raise "n_worker exceeds cpus/node"
    end
  end
end

idx = (0..n_logger).map{|log_id| n_worker*log_id/n_logger}

log_id = 0
z = []
nodes.each do |node_id|
  unless a = cpus[node_id]
    raise "invalid node: #{node_id}"
  end
  n_logger.times do |i|
    log_cpu = a[-1-((n_logger-i-1)*n_logger_core/n_logger)]
    z << log_cpu+':'+a[idx[i]...idx[i+1]].join(',')
    link_dir(node_id,i,log_id)
    log_id += 1
  end
end

puts z.join("+")
