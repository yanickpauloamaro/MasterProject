sudo apt update && sudo apt update

sudo apt install gcc unzip

## Rust and cargo
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

## Install hwloc dependencies
# sudo apt-get install hwloc libhwloc-dev

## gnuplot-x11
#sudo apt install gnuplot_x11

#Zip project
zip backup_number.zip -r testbench/* -x 'testbench/target/*' -x 'testbench/test-path/*' -x 'testbench/other/*'

# Install perf ?

cargo install flamegraph

cargo flamegraph
# or
sudo ../.cargo/bin/flamegraph -- ./target/release/testbench
sudo perf script -F +pid > out.perf