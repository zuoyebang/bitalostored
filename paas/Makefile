GOBUILD=go build

.PHONY: bitalospaas clean buildsucc

.DEFAULT_GOAL := all

all: bitalospaas bitalosagent buildsucc

buildsucc:
	@echo Build Bitalos Pass successfully!

bitalos-deps:
	@mkdir -p bin && bash version

bitalospaas: bitalos-deps
	$(GOBUILD) -o bin/bitalospaas ./main.go

bitalosagent: bitalos-deps
	$(GOBUILD) -o agent/bin/bitalosagent ./agent/main.go

clean:
	@rm -rf bin
