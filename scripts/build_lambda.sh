#!/usr/bin/env bash
# ==============================================================================
# Build Lambda Artifacts
# ==============================================================================
# Creates zip files for all Lambda functions in the artifacts/ directory.
# Run this before deploying with Terraform.
#
# Usage: ./scripts/build_lambda.sh
# ==============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
ARTIFACTS_DIR="$ROOT_DIR/artifacts"

# Create artifacts directory
mkdir -p "$ARTIFACTS_DIR"

echo "Building Lambda artifacts..."

# Build each Lambda function
for lambda in validator processor api_reader; do
    SRC_DIR="$ROOT_DIR/src/$lambda"
    ZIP_PATH="$ARTIFACTS_DIR/$lambda.zip"

    if [ -f "$SRC_DIR/handler.py" ]; then
        echo "  → $lambda.zip"
        zip -j -q "$ZIP_PATH" "$SRC_DIR/handler.py"
    else
        echo "  ✗ $lambda: handler.py not found"
        exit 1
    fi
done

echo ""
echo "Done! Artifacts created in $ARTIFACTS_DIR:"
ls -lh "$ARTIFACTS_DIR"/*.zip
