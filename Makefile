# Copyright 2019 The Bitalostored author and other contributors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

CGOLDFLAGS=CGO_LDFLAGS="-lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -O2"
GOARENAS=GOEXPERIMENT=arenas
GOBUILD=go build

.PHONY: bitalosdashboard bitalosfe bitalosproxy bitalostored clean buildsucc

.DEFAULT_GOAL := all

all: bitalosdashboard bitalosfe bitalosproxy bitalostored buildsucc

buildsucc:
	@echo Build Bitalos successfully!

bitalos-deps:
	@mkdir -p bin && bash version

bitalosdashboard: bitalos-deps
	$(GOBUILD) -o bin/bitalosdashboard ./dashboard/cmd/dashboard

bitalosfe: bitalos-deps
	$(GOBUILD) -o bin/bitalosfe ./dashboard/cmd/fe
	@cp -rf dashboard/cmd/fe-vue/dist bin/

bitalosproxy: bitalos-deps
	CGO_ENABLED=1 $(CGOLDFLAGS) $(GOBUILD) -o bin/bitalosproxy ./proxy/cmd

bitalostored: bitalos-deps
	CGO_ENABLED=1 $(GOARENAS) $(CGOLDFLAGS) $(GOBUILD) -o bin/bitalostored ./stored/cmd

clean:
	@rm -rf bin
	@rm -f proxy/internal/utils/version.go
	@rm -f stored/internal/utils/version.go
	@rm -f dashboard/internal/utils/version.go
