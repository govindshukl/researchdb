"""
Router logic for conditional workflow edges.
Decides whether to skip exploration based on existing view coverage.
"""

import logging
from typing import Literal
from .state import ResearchState

logger = logging.getLogger(__name__)


def should_explore(state: ResearchState) -> Literal["explore", "research"]:
    """
    Decide whether to run Explorer or skip directly to Researcher.

    Args:
        state: Current workflow state

    Returns:
        "explore" to run Explorer, "research" to skip to Researcher
    """
    # Check if we have relevant existing views
    existing_views = state.get('existing_views', [])

    if existing_views and len(existing_views) >= 2:
        # We have sufficient views, skip exploration
        logger.info(
            f"Skipping exploration: {len(existing_views)} relevant views found"
        )
        return "research"

    # Run exploration
    logger.info("Running exploration: no sufficient views found")
    return "explore"


def should_continue_research(state: ResearchState) -> Literal["report", "end"]:
    """
    Decide whether to generate report or end workflow.

    Args:
        state: Current workflow state

    Returns:
        "report" to generate report, "end" to finish
    """
    # Check if we have results to report
    query_results = state.get('query_results', [])
    views_created = state.get('views_created', [])

    if query_results or views_created:
        logger.info("Generating final report")
        return "report"

    # No results, end workflow
    logger.info("No results to report, ending workflow")
    return "end"


def route_on_error(state: ResearchState) -> Literal["report", "end"]:
    """
    Handle error state routing.

    Args:
        state: Current workflow state

    Returns:
        "report" if we can still report partial results, "end" otherwise
    """
    if state.get('error'):
        logger.error(f"Error detected: {state['error']}")

        # Check if we have any partial results
        if state.get('views_created') or state.get('query_results'):
            logger.info("Error occurred but have partial results, generating report")
            return "report"
        else:
            logger.info("Error occurred with no results, ending workflow")
            return "end"

    return "report"


def log_routing_decision(
    from_node: str,
    to_node: str,
    state: ResearchState,
    reason: str = ""
):
    """
    Log routing decisions for observability.

    Args:
        from_node: Source node
        to_node: Destination node
        state: Current state
        reason: Reason for routing decision
    """
    logger.info(
        f"Routing: {from_node} → {to_node} "
        f"(session: {state['session_id']}) "
        f"{reason}"
    )
