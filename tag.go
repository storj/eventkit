package eventkit

import (
	"time"

	"github.com/jtolio/eventkit/pb"
	"google.golang.org/protobuf/types/known/durationpb"
)

type Tag = *pb.Tag

func TagString(key string, val string) Tag {
	return &pb.Tag{
		Key:   key,
		Value: &pb.Tag_String_{String_: val},
	}
}

func TagBytes(key string, val []byte) Tag {
	return &pb.Tag{
		Key:   key,
		Value: &pb.Tag_Bytes{Bytes: val},
	}
}

func TagInt64(key string, val int64) Tag {
	return &pb.Tag{
		Key:   key,
		Value: &pb.Tag_Int64{Int64: val},
	}
}

func TagFloat64(key string, val float64) Tag {
	return &pb.Tag{
		Key:   key,
		Value: &pb.Tag_Double{Double: val},
	}
}

func TagBool(key string, val bool) Tag {
	return &pb.Tag{
		Key:   key,
		Value: &pb.Tag_Bool{Bool: val},
	}
}

func TagDuration(key string, val time.Duration) Tag {
	return &pb.Tag{
		Key:   key,
		Value: &pb.Tag_Duration{Duration: durationpb.New(val)},
	}
}

