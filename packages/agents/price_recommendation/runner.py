"""Agent Runner entrypoint reserved for price recommendation.

Now wraps the OOP AgentPipeline PricePipeline implementation.
"""

from __future__ import annotations

from .pipeline import PricePipeline

__all__ = ["PricePipeline"]
