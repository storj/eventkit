module github.com/jtolio/eventkit/eventkitd

go 1.19

replace github.com/jtolio/eventkit => ../

require (
	github.com/gogo/protobuf v1.3.2
	github.com/jtolio/eventkit v0.0.0-00010101000000-000000000000
	golang.org/x/sync v0.0.0-20220929204114-8fcdb60fdcc0
)

require (
	github.com/google/gopacket v1.1.19
	golang.org/x/net v0.0.0-20201021035429-f5854403a974 // indirect
	golang.org/x/sys v0.0.0-20200930185726-fdedc70b468f // indirect
)
