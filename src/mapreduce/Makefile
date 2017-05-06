
CURRENT_DIR = $(PWD)
seq-test:
	go test -v -run Sequential
wc-test:
	 export "GOPATH=$(CURRENT_DIR)/../.." && cd ../main && go run wc.go master sequential pg-*.txt
clean:
	rm -rf mrtmp.*
	rm -rf 824-*
