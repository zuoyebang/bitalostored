# local output file. Do not upload this file
files="machine_exporter supervisord machine-exporter.toml supervisor.conf load.sh check-env.sh cp-file.sh"
for file in $files
do
    echo "file [$file] is necessary"
done

# machine_exporter supervisord (linux version)