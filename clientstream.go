@@ -30,6 +30,7 @@
 import (
 	"context"
 	"errors"
+	"sync"
 	"time"
 
 	"google.golang.org/grpc/codes"
@@ -45,6 +46,7 @@
 	"google.golang.org/grpc/keepalive"
 	"google.golang.org/grpc/metadata"
 	"google.golang.org/grpc/peer"
+	"google.golang.org/grpc/status"
 )
 
 // ClientStream represents a client-side stream.
@@ -100,7 +101,7 @@
 	if err != nil {
 		return err
 	}
-	return cs.transport.write(ctx, cs.stream, p)
+	return cs.transport.write(ctx, cs.stream, p, cs.ctx)
 }
 
 // CloseSend closes the send direction of the stream.
```

### Summary

- **`internal/transport/http2_client.go`**: Added a function `waitWithCancelCheck` to wait for either the flow control window update or the context cancellation. This function is used in the `write` method to unblock the goroutine and return an error if the context is canceled.
- **`clientstream.go`**: Passed the context to the `write` method in the `SendMsg` function to handle the error returned by the transport layer.

These changes should resolve the issue without introducing race conditions and while maintaining the existing flow control behavior.