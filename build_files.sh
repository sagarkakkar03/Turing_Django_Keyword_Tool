#!/bin/bash

# Build script for Vercel deployment
# Collects static files for Django

# Exit on any error
set -e

echo "Building static files..."
python manage.py collectstatic --noinput

echo "Build complete!"
