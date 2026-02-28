"""Scrapy-based company/leader extraction package."""

from .items import CompanyLeaderItem
from .pipelines import CompanyProfilePipeline

__all__ = ["CompanyLeaderItem", "CompanyProfilePipeline"]
