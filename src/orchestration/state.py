"""
State management for LangGraph orchestration.
Defines the ResearchState that flows through the agent workflow.
"""

from typing import TypedDict, List, Dict, Any, Optional
from typing_extensions import Annotated
import operator


class ResearchState(TypedDict):
    """
    State object for research workflow.
    Flows through: START → EXPLORE → RESEARCH → REPORT → END
    """

    # Input
    user_query: str
    """User's natural language query"""

    user_role: str
    """User role for security context (e.g., 'fraud_analyst')"""

    session_id: str
    """Unique session identifier"""

    # Explorer output
    explorer_result: Optional[Dict[str, Any]]
    """Complete result from Explorer agent"""

    relevant_tables: Annotated[List[str], operator.add]
    """Tables identified as relevant (accumulated)"""

    existing_views: Annotated[List[str], operator.add]
    """View names that already exist (accumulated)"""

    # Researcher output
    researcher_result: Optional[Dict[str, Any]]
    """Complete result from Researcher agent"""

    query_results: Annotated[List[Dict[str, Any]], operator.add]
    """Analytical query results (accumulated)"""

    # View tracking
    views_created: Annotated[List[str], operator.add]
    """View names created during this session (accumulated)"""

    views_used: Annotated[List[str], operator.add]
    """View names reused from catalog (accumulated)"""

    # Final output
    final_report: Optional[str]
    """Final research report for user"""

    analysis: Optional[str]
    """Analytical findings summary"""

    # Workflow control
    skip_exploration: bool
    """Flag to skip Explorer if views already exist"""

    # Metadata
    start_time: Optional[str]
    """Workflow start timestamp"""

    end_time: Optional[str]
    """Workflow end timestamp"""

    error: Optional[str]
    """Error message if workflow fails"""

    current_step: str
    """Current workflow step for debugging"""


def create_initial_state(
    user_query: str,
    user_role: str = "analyst",
    session_id: Optional[str] = None
) -> ResearchState:
    """
    Create initial state for a new research session.

    Args:
        user_query: User's natural language query
        user_role: User's role for security context
        session_id: Optional session ID (generated if not provided)

    Returns:
        Initial ResearchState object
    """
    import uuid
    from datetime import datetime

    if not session_id:
        session_id = f"session_{uuid.uuid4().hex[:8]}"

    return ResearchState(
        # Input
        user_query=user_query,
        user_role=user_role,
        session_id=session_id,

        # Explorer output
        explorer_result=None,
        relevant_tables=[],
        existing_views=[],

        # Researcher output
        researcher_result=None,
        query_results=[],

        # View tracking
        views_created=[],
        views_used=[],

        # Final output
        final_report=None,
        analysis=None,

        # Workflow control
        skip_exploration=False,

        # Metadata
        start_time=datetime.utcnow().isoformat(),
        end_time=None,
        error=None,
        current_step="START"
    )


def validate_state(state: ResearchState) -> bool:
    """
    Validate that state has required fields.

    Args:
        state: State to validate

    Returns:
        True if valid, False otherwise
    """
    required_fields = ['user_query', 'user_role', 'session_id']

    for field in required_fields:
        if field not in state or not state[field]:
            return False

    return True


def format_state_summary(state: ResearchState) -> str:
    """
    Format state as readable summary for debugging/logging.

    Args:
        state: Current state

    Returns:
        Formatted summary string
    """
    lines = [
        f"Session: {state['session_id']}",
        f"Query: {state['user_query']}",
        f"Role: {state['user_role']}",
        f"Step: {state['current_step']}",
        "",
        f"Tables: {len(state.get('relevant_tables', []))}",
        f"Views Created: {len(state.get('views_created', []))}",
        f"Views Used: {len(state.get('views_used', []))}",
        f"Queries Executed: {len(state.get('query_results', []))}",
    ]

    if state.get('error'):
        lines.append(f"Error: {state['error']}")

    if state.get('final_report'):
        lines.append(f"Report: {len(state['final_report'])} chars")

    return "\n".join(lines)
