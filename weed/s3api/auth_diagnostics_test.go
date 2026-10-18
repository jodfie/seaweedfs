package s3api

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/seaweedfs/seaweedfs/weed/util/fla9"
)

// A subprocess captures the actual logger without replacing global stderr or
// logger state under concurrent package tests. Exercise both absent and expired
// credentials: retirement must neither change denial nor disclose key identity.
func TestAuthenticationDiagnosticsExcludeCredentials(t *testing.T) {
	const access = "CPSAUTHLOGSENTINEL123456789"
	const secret = "CPS_AUTH_SECRET_SENTINEL_NOT_FOR_LOGS"
	if os.Getenv("SEAWEED_AUTH_DIAGNOSTIC_HELPER") == "1" {
		if fla9.Set("logtostderr", "true") != nil || fla9.Set("v", "2") != nil {
			t.Fatal("fixture logger setup failed")
		}
		for _, expired := range []bool{false, true} {
			iam := setupTestIAMForV2Auth()
			want := s3err.ErrInvalidAccessKeyID
			if expired {
				credential := &Credential{AccessKey: access, SecretKey: secret, Expiration: time.Now().Add(-time.Hour).Unix()}
				identity := &Identity{Name: "synthetic-expired", Credentials: []*Credential{credential}}
				iam.accessKeyIdent[access] = identity
				want = s3err.ErrAccessDenied
			}
			check := func(code s3err.ErrorCode) {
				if code != want {
					t.Fatalf("authentication denial changed: %d, want %d", code, want)
				}
			}
			v2 := httptest.NewRequest(http.MethodGet, "http://local-s3/bucket/object", nil)
			v2.Header.Set("Authorization", "AWS "+access+":synthetic-signature")
			_, code := iam.doesSignV2Match(v2)
			check(code)
			query := v2.URL.Query()
			query.Set("AWSAccessKeyId", access)
			query.Set("Signature", "synthetic-signature")
			query.Set("Expires", strconv.FormatInt(time.Now().Add(time.Hour).Unix(), 10))
			v2.URL.RawQuery = query.Encode()
			_, code = iam.doesPresignV2SignatureMatch(v2)
			check(code)
			form := http.Header{}
			form.Set("AWSAccessKeyId", access)
			_, code = iam.doesPolicySignatureV2Match(form)
			check(code)
			v4 := httptest.NewRequest(http.MethodGet, "http://local-s3/bucket/object", nil)
			v4.Header.Set("X-Amz-Content-Sha256", unsignedPayload)
			if signRequestV4(v4, access, secret) != nil {
				t.Fatal("fixture signing failed")
			}
			_, _, _, _, code = iam.verifyV4Signature(v4, false)
			check(code)
			presigned := httptest.NewRequest(http.MethodGet, "http://local-s3/bucket/object", nil)
			if preSignV4(iam, presigned, access, secret, 60) != nil {
				t.Fatal("fixture presigning failed")
			}
			_, _, _, _, code = iam.verifyV4Signature(presigned, false)
			check(code)
			form.Set("X-Amz-Credential", access+"/"+time.Now().UTC().Format(yyyymmdd)+"/us-east-1/s3/aws4_request")
			_, code = iam.doesPolicySignatureV4Match(form)
			check(code)
		}
		glog.Flush()
		fmt.Println("AUTH_DIAGNOSTICS_CHECKED")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestAuthenticationDiagnosticsExcludeCredentials$")
	command.Env = append(os.Environ(), "SEAWEED_AUTH_DIAGNOSTIC_HELPER=1")
	output, err := command.CombinedOutput()
	if err != nil || !bytes.Contains(output, []byte("AUTH_DIAGNOSTICS_CHECKED")) {
		t.Fatal("authentication diagnostic subprocess failed")
	}
	if bytes.Contains(output, []byte(access)) || bytes.Contains(output, []byte(secret)) {
		t.Fatal("authentication diagnostic disclosed credential material")
	}
	if !bytes.Contains(output, []byte("InvalidAccessKeyId")) || !bytes.Contains(output, []byte("expired")) {
		t.Fatal("authentication diagnostics were suppressed")
	}
}
