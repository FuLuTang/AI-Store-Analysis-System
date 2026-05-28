"""Agent Runner entrypoint reserved for price recommendation.

The first implementation uses a deterministic workflow in
packages.price_recommendation.workflow. This module defines the future boundary
for calling AgentLoop without making the business workflow depend on diagnosis.
"""

from __future__ import annotations


class PriceAgentRunner:
    def __init__(self, *_, **__):
        pass

    def run(self):
        raise NotImplementedError("PriceAgentRunner is reserved for the next implementation step.")
