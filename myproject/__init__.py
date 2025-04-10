from __future__ import absolute_import, unicode_literals

# This will import the Celery app instance
from .celery import app as celery_app

__all__ = ('celery_app',)