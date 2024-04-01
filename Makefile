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

CGO_LDFLAGS=CGO_LDFLAGS="-lstdc++ -O2"
GOBUILD=go build
GOCGOBUILD=CGO_ENABLED=1 $(CGO_LDFLAGS) $(GOBUILD)

.PHONY: bitalosproxy bitalostored clean buildsucc bitalosdashboard bitalosfe

.DEFAULT_GOAL := all

all: bitalosproxy bitalostored bitalosdashboard bitalosfe buildsucc

buildsucc:
	@echo Build Bitalos successfully!

bitalos-deps:
	@mkdir -p bin && bash version

bitalosproxy: bitalos-deps
	$(GOCGOBUILD) -o bin/bitalosproxy ./proxy/cmd

bitalostored: bitalos-deps
	GOEXPERIMENT=arenas $(GOCGOBUILD) -o bin/bitalostored ./stored/cmd

bitalosdashboard: bitalos-deps
	$(GOCGOBUILD) -o bin/bitalosdashboard ./dashboard/cmd/dashboard

bitalosfe: bitalos-deps
	$(GOCGOBUILD) -o bin/bitalosfe ./dashboard/cmd/fe
	@cp -rf dashboard/cmd/fe-vue/dist bin/

clean:
	@rm -rf bin
	@rm -f proxy/internal/utils/version.go
	@rm -f stored/internal/utils/version.go
	@rm -f dashboard/internal/utils/version.go
