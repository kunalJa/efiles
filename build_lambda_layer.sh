#!/bin/bash
# Build the image-generation Lambda layer using the AWS SAM Python image
#
# Usage:
#   chmod +x build_lambda_layer.sh
#   ./build_lambda_layer.sh
#
# Output: print-images-layer.zip (upload to AWS Lambda Layers)

set -e

LAYER_NAME="print-images-layer"
PYTHON_VERSION="3.13"

echo "Building Lambda layer for PyMuPDF and Pillow..."

# Create temp directory (use sudo to clean up root-owned files from Docker)
sudo rm -rf layer_build 2>/dev/null || rm -rf layer_build
mkdir -p layer_build/python

# Build x86_64 Linux wheels matching the Lambda runtime
docker run --rm --platform linux/amd64 -v "$(pwd)/layer_build:/out" \
    public.ecr.aws/sam/build-python${PYTHON_VERSION}:latest \
    pip install "PyMuPDF==1.28.2" "Pillow==12.2.0" -t /out/python

# Fix permissions so we can zip (Docker creates as root)
sudo chown -R $(id -u):$(id -g) layer_build

# Create the zip on host
cd layer_build
zip -r9 ../${LAYER_NAME}.zip python
cd ..

# Cleanup (use sudo for root-owned Docker files)
sudo rm -rf layer_build 2>/dev/null || rm -rf layer_build

echo ""
echo "✅ Created: ${LAYER_NAME}.zip"
echo ""
echo "Next steps:"
echo "  1. Go to AWS Lambda Console > Layers > Create Layer"
echo "  2. Upload ${LAYER_NAME}.zip"
echo "  3. Compatible runtimes: Python ${PYTHON_VERSION}"
echo "  4. Add this layer to your Lambda function"
