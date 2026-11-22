# GeoStream AWS Deployment Notes

Declarative AWS infrastructure lives in `../../deploy/terraform/aws/main.tf` (private S3 artifacts bucket with 14-day TTL, ECR repository, 7-day CloudWatch log group, and least-privilege IAM role).

All cloud execution and EKS/MSK provisioning remain **`BLOCKED_EXTERNAL`** (`../../CLOUD_APPROVAL.md`) absent IAM credentials and explicit spend approval. Local Docker Compose + `kubectl apply --dry-run=client` verify runtime behavior and Kubernetes manifest validity cleanly.

