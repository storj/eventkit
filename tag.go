package eventkit

import (
	"time"

	"github.com/jtolio/eventkit/pb"
	"google.golang.org/protobuf/types/known/durationpb"
)

type Tag = *pb.Tag

func String(key string, val string) Tag {
	return &pb.Tag{
		Key:   key,
		Value: &pb.Tag_String_{String_: val},
	}
}

func Bytes(key string, val []byte) Tag {
	return &pb.Tag{
		Key:   key,
		Value: &pb.Tag_Bytes{Bytes: val},
	}
}

func Int64(key string, val int64) Tag {
	return &pb.Tag{
		Key:   key,
		Value: &pb.Tag_Int64{Int64: val},
	}
}

func Float64(key string, val float64) Tag {
	return &pb.Tag{
		Key:   key,
		Value: &pb.Tag_Double{Double: val},
	}
}

func Bool(key string, val bool) Tag {
	return &pb.Tag{
		Key:   key,
		Value: &pb.Tag_Bool{Bool: val},
	}
}

func Duration(key string, val time.Duration) Tag {
	return &pb.Tag{
		Key:   key,
		Value: &pb.Tag_Duration{Duration: durationpb.New(val)},
	}
}
