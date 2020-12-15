require 'csv'
require 'numo/gnuplot'

titles = {0=>'no logging',8=>'8 loggers',16=>'16 loggers',24=>'24 loggers'}
dt = {0=>'',8=>'dt "-"',16=>'dt "."',24=>'dt "-."'}
col = {8=>"red",16=>"green",24=>"blue"}
pt = {2=>2,3=>8,4=>6,8=>12}

f = ARGV[0]
d = {}
CSV.foreach(f,headers:true) do |row|
  l = row['logger_num'].to_i
  t = row['thread_num'].to_i
  if l > 0 && l+t == 224
    n = row['buffer_num'].to_i
    e = (d[[l,n]] ||= {})
    s = row['buffer_size'].to_i
    (e[s] ||= []) << row['throughput[tps]'].to_i*1e-6
  end
end

dat = []
d.keys.sort.each do |k|
  l,n = k
  dat << lin = [[],[],w:"lp #{dt[l]} pt #{pt[n]} lc rgbcolor \"#{col[l]}\"",
                title:" N_L=#{l}, N_B=#{n}"]
  e = d[k]
  e.keys.sort.each do |sz|
    lin[0] << sz
    a = e[sz]
    lin[1] << a.sort[a.size/2]
  end
end

outfile=File.basename(f,'.csv')

%w[png eps emf].each do |term|
  Numo.gnuplot do
    set encoding:"iso_8859_1"
    case term
    when 'png'
      set term:"pngcairo"
      set output: "#{outfile}.png"
    when 'eps'
      set term:"postscript eps color enhanced", size:'4 inch, 2.5 inch',
          font:["Helvetica",16]
        set output: "#{outfile}.eps"
    when 'emf'
      set term:"emf color enhanced", size:'864,648',
          font:["Helvetica",16]
        set output: "#{outfile}.emf"
    end
    set xlabel:"Buffer size [KiB]"
    set ylabel:'"throughput [Mtps]"'
    set title:'Silo YCSB write=100%, 224 threads(worker+logger),
N_L (# of loggers), N_B (# of buffers)'
    set xrange:0..2500
    set yrange:20..40
    #set :nokey
    set key:'outside'
    plot(*dat)
  end
end
