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
 
 // http2Client implements the Transport interface.
@@ -100,6 +102,10 @@
 	}
 }
 
+// waitWithCancelCheck waits for the flow control window to be updated or for the context to be canceled.
+func (t *http2Client) waitWithCancelCheck(ctx context.Context, wq *writeQuota) error {
+	select {
+	case <-ctx.Done():
+		return status.Errorf(codes.Canceled, "context canceled while waiting for flow control window update")
+	case <-wq.update:
+		return nil
+	}
 }
 
 // write writes data to the stream.
@@ -120,7 +126,7 @@
 	if err != nil {
 		return err
 	}
-	wq.wait()
+	if err := t.waitWithCancelCheck(ctx, wq); err != nil {
+		return err
 	}
 	return nil
 }
```

#### 2. Modify `clientstream.go`

We need to pass the context to the transport layer and handle the error returned by `SendMsg`.

```diff
--- a/clientstream.go