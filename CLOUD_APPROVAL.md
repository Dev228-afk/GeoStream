# CLOUD_APPROVAL.md — GeoStream

- **Deployment Status**: `BLOCKED_EXTERNAL` (`aws` CLI absent, `kind`/`helm` absent, no IAM credentials, no spend authorization).
- **Kubernetes Validation**: `deploy/k8s/geostream.yaml` validated statically via `kubectl apply --dry-run=client`.
- **Target Region**: `us-west-2`
- **Services (Cost-Bounded Profile)**: ECR (`geostream-demo`), single-cluster EC2/EKS dev worker nodes running KRaft Kafka (cheaper dev option vs managed MSK), S3 benchmark artifact archive (`block_public_* = true`, 14-day TTL), CloudWatch logs (7-day retention).
- **Estimated Cost**: ~$0.12/hour active (`< $0.50` for 2–3 hour stress benchmark window).
- **Maximum Test Duration**: 2 hours followed immediately by `terraform destroy -auto-approve`.
- **Privacy Policy**: Courier routes are 100% synthetic coordinates (`37.33, -121.89` grid); zero real device/location PII.
- **Teardown Command**: `cd deploy/terraform/aws && terraform destroy -auto-approve`
- **Required Action to Unblock**: Install `aws`/`terraform`/`helm`, export valid IAM credentials, and authorize spend.

