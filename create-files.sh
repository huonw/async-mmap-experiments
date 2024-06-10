rm -rf large-files
mkdir -p large-files

for i in $(seq -w 1 32); do
    echo $i
    fname="large-files/file.${i}"
    rm -f $fname
    dd if=/dev/zero of=$fname ibs=1k  count=256k
done
