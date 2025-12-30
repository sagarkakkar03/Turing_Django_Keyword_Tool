#!/bin/bash

# Build script for Vercel deployment
# Collects static files for Django

# Exit on any error
set -e

echo "Building static files..."

# Create static directory if it doesn't exist
mkdir -p staticfiles_build/static

# Run collectstatic with DJANGO_SETTINGS_MODULE explicitly set
DJANGO_SETTINGS_MODULE=myproject.settings python manage.py collectstatic --noinput || true

echo "Build complete!"
