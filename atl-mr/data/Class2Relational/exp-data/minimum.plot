set xlabel "mappers (splits)"
set ylabel "min time(s)"

set xrange [1:9]
set yrange [0:900]

set ytics nomirror


plot "timings-class-10000-min.txt" using 1:2 title "10000v" with lines, \
     "timings-class-20000-min.txt" using 1:2 title "20000v" with lines, \
     "timings-class-30000-min.txt" using 1:2 title "30000v" with lines, \
     "timings-class-40000-min.txt" using 1:2 title "40000v" with lines, \
     "timings-class-50000-min.txt" using 1:2 title "50000v" with lines, \
     "timings-class-50000-min.txt" using 1:2 title "50000v" with lines,



set size 1, 0.45
set terminal postscript portrait enhanced color dashed lw 1 "Helvetica" 14
set output "exp-min.ps"
replot
set term pop
