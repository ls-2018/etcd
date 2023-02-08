module github.com/ls-2018/etcd_cn

go 1.16

replace go.etcd.io/etcd/api/v3 v3.5.2 => github.com/etcd-io/etcd/api/v3 v3.5.2

replace github.com/etcd-io/etcd/api/v3 v3.5.2 => ./offical/api/v3

replace go.etcd.io/etcd/client/v3 v3.5.2 => github.com/etcd-io/etcd/client/v3 v3.5.2

replace github.com/etcd-io/client/api/v3 v3.5.2 => ./offical/client/v3
replace github.com/ls-2018/etcd_cn/official => ./offical
require (
	github.com/akhenakh/hunspellgo v0.0.0-20160221122622-9db38fa26e19 // indirect
	github.com/alexkohler/nakedret v1.0.0
	github.com/bgentry/speakeasy v0.1.0
	github.com/chzchzchz/goword v0.0.0-20170907005317-a9744cb52b03
	github.com/coreos/go-semver v0.3.0
	github.com/coreos/go-systemd/v22 v22.3.2
	github.com/coreos/license-bill-of-materials v0.0.0-20190913234955-13baff47494e
	github.com/creack/pty v1.1.11
	github.com/dustin/go-humanize v1.0.0
	github.com/fatih/color v1.10.0 // indirect
	github.com/form3tech-oss/jwt-go v3.2.3+incompatible
	github.com/go-openapi/loads v0.19.5 // indirect
	github.com/go-openapi/spec v0.19.9 // indirect
	github.com/gogo/protobuf v1.3.2
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da
	github.com/golang/protobuf v1.5.2
	github.com/google/btree v1.0.1
	github.com/gordonklaus/ineffassign v0.0.0-20200809085317-e36bfde3bb78
	github.com/grpc-ecosystem/go-grpc-middleware v1.3.0
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/grpc-ecosystem/grpc-gateway v1.16.0
	github.com/gyuho/gocovmerge v0.0.0-20171205171859-50c7e6afd535
	github.com/hexfusion/schwag v0.0.0-20170606222847-b7d0fc9aadaa
	github.com/jonboulle/clockwork v0.2.2
	github.com/json-iterator/go v1.1.11
	github.com/kr/pretty v0.3.0 // indirect
	github.com/mattn/go-colorable v0.1.12 // indirect
	github.com/mdempsky/unconvert v0.0.0-20200228143138-95ecdbfc0b5f
	github.com/mgechev/revive v1.0.2
	github.com/mikefarah/yq/v3 v3.0.0-20201125113350-f42728eef735
	github.com/modern-go/reflect2 v1.0.1
	github.com/olekukonko/tablewriter v0.0.5
	github.com/prometheus/client_golang v1.11.0
	github.com/rogpeppe/go-internal v1.8.1 // indirect
	github.com/sirupsen/logrus v1.7.0 // indirect
	github.com/soheilhy/cmux v0.1.5
	github.com/spf13/cobra v1.1.3
	github.com/spf13/pflag v1.0.5
	github.com/stretchr/testify v1.7.0
	github.com/tmc/grpc-websocket-proxy v0.0.0-20201229170055-e5319fda7802
	github.com/trustmaster/go-aspell v0.0.0-20200701131845-c2b1f55bec8f // indirect
	github.com/xiang90/probing v0.0.0-20190116061207-43a291ad63a2
	go.etcd.io/bbolt v1.3.6
	go.etcd.io/protodoc v0.0.0-20180829002748-484ab544e116
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.20.0
	go.opentelemetry.io/otel v0.20.0
	go.opentelemetry.io/otel/exporters/otlp v0.20.0
	go.opentelemetry.io/otel/sdk v0.20.0
	go.uber.org/multierr v1.6.0
	go.uber.org/zap v1.17.0
	golang.org/x/crypto v0.0.0-20210921155107-089bfa567519
	golang.org/x/net v0.0.0-20211008194852-3b03d305991f
	golang.org/x/sys v0.0.0-20220307203707-22a9840ba4d7
	golang.org/x/text v0.3.7 // indirect
	golang.org/x/time v0.0.0-20210220033141-f8bda1e9f3ba
	google.golang.org/genproto v0.0.0-20210624195500-8bfb893ecb84
	google.golang.org/grpc v1.38.0
	gopkg.in/cheggaaa/pb.v1 v1.0.28
	gopkg.in/natefinch/lumberjack.v2 v2.0.0
	honnef.co/go/tools v0.0.1-2019.2.3
	mvdan.cc/unparam v0.0.0-20200501210554-b37ab49443f7
	sigs.k8s.io/yaml v1.2.0
)
