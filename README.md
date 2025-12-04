# BatchFactory Solution

A serverless, event-driven CSV processing pipeline on AWS, deployed with Terraform.

📋 [View Challenge](https://hyperoot.dev/challenges/batchfactory-aws) · 📖 [Read Solution Guide](https://hyperoot.dev/solutions/batchfactory-aws)

## Architecture

![Architecture](./assets/batchfactory.svg)

## Quick Start

### Prerequisites

- AWS CLI configured with appropriate credentials
- Terraform installed
- Python 3.11

### Deploy

```bash
# 1. Build Lambda artifacts
./scripts/build_lambda.sh

# 2. Initialize Terraform
cd terraform/environments/dev
terraform init

# 3. Deploy infrastructure
terraform apply
```

### Test

```bash
# Upload a test file
./scripts/upload_file.sh samples/sample-small.csv

# Check job status (replace <api-url> with your API Gateway URL)
curl https://<api-url>/dev/jobs/sample-small
```

### Destroy

```bash
cd terraform/environments/dev
terraform destroy
```

---

## Project Structure

```
├── src/                  # Lambda function code
├── terraform/            # Infrastructure as Code
├── samples/              # Test CSV files
└── scripts/              # Build and deployment scripts
```

## License

See [LICENSE](./LICENSE) for details.

## Support

If you find this helpful, consider supporting my work ❤️

<a href="https://github.com/sponsors/HYP3R00T"><img src="https://img.shields.io/badge/Sponsor-GitHub-ea4aaa?style=for-the-badge&logo=github" alt="Sponsor on GitHub"></a>

