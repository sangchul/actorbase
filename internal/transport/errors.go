package transport

import (
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/oomymy/actorbase/provider"
)

// toGRPCStatus는 provider error를 gRPC status error로 변환한다.
// 알 수 없는 에러는 INTERNAL로 변환한다.
func toGRPCStatus(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, provider.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, provider.ErrPartitionMoved):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, provider.ErrPartitionNotOwned):
		return status.Error(codes.Unavailable, err.Error())
	case errors.Is(err, provider.ErrPartitionBusy):
		return status.Error(codes.ResourceExhausted, err.Error())
	case errors.Is(err, provider.ErrTimeout):
		return status.Error(codes.DeadlineExceeded, err.Error())
	case errors.Is(err, provider.ErrActorPanicked):
		return status.Error(codes.Internal, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

// fromGRPCStatus는 gRPC status error를 provider error로 변환한다.
// 알 수 없는 status는 그대로 반환한다.
func fromGRPCStatus(err error) error {
	if err == nil {
		return nil
	}
	st, ok := status.FromError(err)
	if !ok {
		return err
	}
	switch st.Code() {
	case codes.NotFound:
		return provider.ErrNotFound
	case codes.FailedPrecondition:
		return provider.ErrPartitionMoved
	case codes.Unavailable:
		return provider.ErrPartitionNotOwned
	case codes.ResourceExhausted:
		return provider.ErrPartitionBusy
	case codes.DeadlineExceeded:
		return provider.ErrTimeout
	case codes.Internal:
		return provider.ErrActorPanicked
	default:
		return err
	}
}
