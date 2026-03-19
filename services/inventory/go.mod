module github.com/red-star25/quickbite/inventory

go 1.25.8

replace github.com/red-star25/quickbite/proto => ../../proto

require (
	github.com/jackc/pgx/v5 v5.8.0
	github.com/red-star25/quickbite/proto v0.0.0-00010101000000-000000000000
	github.com/segmentio/kafka-go v0.4.50
	google.golang.org/grpc v1.79.2
)

require (
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/klauspost/compress v1.15.9 // indirect
	github.com/pierrec/lz4/v4 v4.1.15 // indirect
	golang.org/x/net v0.51.0 // indirect
	golang.org/x/sync v0.19.0 // indirect
	golang.org/x/sys v0.42.0 // indirect
	golang.org/x/text v0.34.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260226221140-a57be14db171 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)
