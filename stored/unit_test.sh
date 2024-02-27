cd engine/bitsdb/bitsdb/locker
go test -v -timeout 2000s
cd -

cd engine/bitsdb/bitsdb
go test -v -timeout 2000s
cd -

cd engine
go test -v -timeout 2000s
cd -