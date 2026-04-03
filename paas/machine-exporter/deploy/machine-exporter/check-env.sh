dir=/home/homework/prometheus/machine-exporter

if [ ! -d $dir ]
then
    echo "$dir not found"
    mkdir -p $dir
    echo "$dir is created"
fi

files="machine-exporter.toml"
for file in $files
do
    if [ ! -f $dir/conf/$file ]
    then
        echo "file $file not found"
        exit 0
    fi
done

files="machine_exporter supervisord supervisor.conf load.sh"
for file in $files
do
    if [ ! -f $dir/bin/$file ]
    then
        echo "file $file not found"
        exit 0
    fi
done

echo "=====output conf====="
cat $dir/conf/machine-exporter.toml

echo "=====check process status====="
cd $dir/bin
bash load.sh status