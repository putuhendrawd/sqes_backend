"""
Workflows package for SQES.

This package contains the main processing workflows for seismic quality evaluation.
"""
from .orchestrator import run_processing_workflow

__all__ = ['run_processing_workflow']
