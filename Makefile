get_protogen:
	go get -u github.com/golang/protobuf/protoc-gen-go

protogen: get_protogen
	protoc --go_out=$(shell pwd) antidote.proto
