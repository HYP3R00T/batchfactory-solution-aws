#!/usr/bin/env bash
set -euo pipefail

BUCKET="$1"
FILE="$2"

if [[ -z "$BUCKET" || -z "$FILE" ]]; then
  echo "Usage: $0 <s3-bucket> <file.csv>"
  exit 1
fi

aws s3 cp "$FILE" "s3://${BUCKET}/uploads/$(basename "$FILE")"
echo "Uploaded $FILE to s3://$BUCKET/uploads/$(basename "$FILE")"
