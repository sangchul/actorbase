package transport

import (
	"errors"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/sangchul/actorbase/provider"
)

// toGRPCStatus converts a provider error to a gRPC status error.
// Unknown errors are converted to INTERNAL.
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

// fromGRPCStatus converts a gRPC status error to a provider error.
// Unknown status codes are returned as-is.
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
	case codes.PermissionDenied:
		return fmt.Errorf("%s", st.Message())
	case codes.Internal:
		// Only ErrActorPanicked is encoded as codes.Internal.
		// Other Internal codes are server-side errors; preserve their message.
		if errors.Is(provider.ErrActorPanicked, fmt.Errorf("%s", st.Message())) ||
			st.Message() == provider.ErrActorPanicked.Error() {
			return provider.ErrActorPanicked
		}
		return fmt.Errorf("internal server error: %s", st.Message())
	default:
		return err
	}
}
