module github.com/uded/koanf-etcd

go 1.25.0

toolchain go1.25.11

retract (
	v0.2.1 // TestClose_HonorsCloseTimeout upper bound was too tight for loaded CI runners; use v0.2.2 or later.
	v0.2.0 // gofmt drift in settings.go and watch.go made CI red; use v0.2.2 or later.
)

require (
	github.com/knadh/koanf/maps v0.1.2
	github.com/knadh/koanf/parsers/yaml v1.1.0
	github.com/knadh/koanf/v2 v2.3.5
	go.etcd.io/etcd/api/v3 v3.5.31
	go.etcd.io/etcd/client/v3 v3.5.31
	google.golang.org/grpc v1.81.1
)

require (
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd/v22 v22.3.2 // indirect
	github.com/go-viper/mapstructure/v2 v2.4.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/mitchellh/copystructure v1.2.0 // indirect
	github.com/mitchellh/reflectwalk v1.0.2 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/rogpeppe/go-internal v1.10.0 // indirect
	go.etcd.io/etcd/client/pkg/v3 v3.5.31 // indirect
	go.uber.org/atomic v1.7.0 // indirect
	go.uber.org/multierr v1.6.0 // indirect
	go.uber.org/zap v1.17.0 // indirect
	go.yaml.in/yaml/v3 v3.0.3 // indirect
	golang.org/x/net v0.55.0 // indirect
	golang.org/x/sys v0.45.0 // indirect
	golang.org/x/text v0.37.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20260226221140-a57be14db171 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260226221140-a57be14db171 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
)
