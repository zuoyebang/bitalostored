# change to your own directory
src_dir={YOUR_DIR}
dst_dir=/home/homework/prometheus/machine-exporter
mkdir -p $dst_dir
mkdir -p $dst_dir/bin
mkdir -p $dst_dir/conf

files="machine-exporter.toml"
for file in $files
do
    cp $src_dir/$file $dst_dir/conf/$file
done

files="supervisord supervisor.conf load.sh"
for file in $files
do
    cp $src_dir/$file $dst_dir/bin/$file
done