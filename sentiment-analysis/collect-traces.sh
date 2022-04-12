TRACE_DIR=/disk/local/s1897969/full-faas-apps-tracedir/sentiment-analysis/
export OMP_NUM_THREADS=1
export MKL_NUM_THREADS=1

gcc -fpic `python3-config --cflags` -o unified1 unified1.c  `python3-config --ldflags` -DADARSH11
~/prism/build/bin/prism --backend=stgen -l textv2 -o $TRACE_DIR/readcsv/get --frontend=valgrind --start-func=roi_begin --stop-func=roi_end --executable="./unified1"
gcc -fpic `python3-config --cflags` -o unified1 unified1.c  `python3-config --ldflags` -DADARSH12
~/prism/build/bin/prism --backend=stgen -l textv2 -o $TRACE_DIR/readcsv/compute --frontend=valgrind --start-func=roi_begin --stop-func=roi_end --executable="./unified1"
gcc -fpic `python3-config --cflags` -o unified1 unified1.c  `python3-config --ldflags` -DADARSH13
~/prism/build/bin/prism --backend=stgen -l textv2 -o $TRACE_DIR/readcsv/put --frontend=valgrind --start-func=roi_begin --stop-func=roi_end --executable="./unified1"

gcc -fpic `python3-config --cflags` -o unified1 unified1.c  `python3-config --ldflags` -DADARSH21
~/prism/build/bin/prism --backend=stgen -l textv2 -o $TRACE_DIR/sentiment/get --frontend=valgrind --start-func=roi_begin --stop-func=roi_end --executable="./unified1"
gcc -fpic `python3-config --cflags` -o unified1 unified1.c  `python3-config --ldflags` -DADARSH22
~/prism/build/bin/prism --backend=stgen -l textv2 -o $TRACE_DIR/sentiment/compute --frontend=valgrind --start-func=roi_begin --stop-func=roi_end --executable="./unified1"
gcc -fpic `python3-config --cflags` -o unified1 unified1.c  `python3-config --ldflags` -DADARSH23
~/prism/build/bin/prism --backend=stgen -l textv2 -o $TRACE_DIR/sentiment/put --frontend=valgrind --start-func=roi_begin --stop-func=roi_end --executable="./unified1"

gcc -fpic `python3-config --cflags` -o unified1 unified1.c  `python3-config --ldflags` -DADARSH31
~/prism/build/bin/prism --backend=stgen -l textv2 -o $TRACE_DIR/publish/get --frontend=valgrind --start-func=roi_begin --stop-func=roi_end --executable="./unified1"
gcc -fpic `python3-config --cflags` -o unified1 unified1.c  `python3-config --ldflags` -DADARSH32
~/prism/build/bin/prism --backend=stgen -l textv2 -o $TRACE_DIR/publish/compute --frontend=valgrind --start-func=roi_begin --stop-func=roi_end --executable="./unified1"

gcc -fpic `python3-config --cflags` -o unified1 unified1.c  `python3-config --ldflags` -DADARSH41
~/prism/build/bin/prism --backend=stgen -l textv2 -o $TRACE_DIR/writedb/get --frontend=valgrind --start-func=roi_begin --stop-func=roi_end --executable="./unified1"
gcc -fpic `python3-config --cflags` -o unified1 unified1.c  `python3-config --ldflags` -DADARSH42
~/prism/build/bin/prism --backend=stgen -l textv2 -o $TRACE_DIR/writedb/compute --frontend=valgrind --start-func=roi_begin --stop-func=roi_end --executable="./unified1"
