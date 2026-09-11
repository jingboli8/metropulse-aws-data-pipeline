"""Thin AWS-facing adapters for daily MetroPulse object processing."""

from metropulse.aws.config import LambdaConfig
from metropulse.aws.processor import ObjectProcessor, ProcessingResult

__all__ = ["LambdaConfig", "ObjectProcessor", "ProcessingResult"]
