"""
LangGraph workflow for multi-agent research orchestration.
Defines the StateGraph with Explorer → Researcher → Report flow.
"""

import logging
from datetime import datetime
from typing import Dict, Any
from langgraph.graph import StateGraph, END

from .state import ResearchState, create_initial_state, format_state_summary
from .router import should_explore, should_continue_research, log_routing_decision
from ..agents.explorer_agent import ExplorerAgent
from ..agents.researcher_agent import ResearcherAgent
from ..database.connection import get_db

logger = logging.getLogger(__name__)


def start_node(state: ResearchState) -> ResearchState:
    """
    Initialize the research workflow.

    Args:
        state: Initial state

    Returns:
        Updated state
    """
    logger.info(f"Starting research session: {state['session_id']}")
    logger.info(f"User query: {state['user_query']}")

    state['current_step'] = "START"
    state['start_time'] = datetime.utcnow().isoformat()

    return state


def explore_node(state: ResearchState) -> ResearchState:
    """
    Run Explorer agent to create Layer 1 discovery views.

    Args:
        state: Current state

    Returns:
        Updated state with Explorer results
    """
    logger.info("=" * 80)
    logger.info("EXPLORE NODE - Starting")
    logger.info("=" * 80)
    state['current_step'] = "EXPLORE"

    try:
        # Initialize Explorer
        db = get_db()
        explorer = ExplorerAgent(db=db, role=state['user_role'])
        logger.info(f"Explorer initialized for role: {state['user_role']}")

        # Process query
        logger.info(f"Processing query: {state['user_query']}")
        result = explorer.process(state['user_query'])
        logger.info(f"Explorer process returned: success={result.get('success')}")

        # Update state
        state['explorer_result'] = result

        if result['success']:
            # Extract context
            context = result.get('context', {})
            logger.info(f"Context keys: {list(context.keys())}")

            # Accumulate tables and views
            if 'relevant_tables' in context:
                state['relevant_tables'] = context['relevant_tables']
                logger.info(f"Relevant tables: {context['relevant_tables']}")

            if 'existing_views' in context:
                state['existing_views'] = context['existing_views']
                logger.info(f"Existing views found: {len(context['existing_views'])}")

            if 'created_views' in context:
                # Add created view names
                created_view_names = [v.view_name for v in result.get('created_views', [])]
                state['views_created'] = created_view_names
                logger.info(f"Views created by Explorer: {created_view_names}")

            logger.info(
                f"Explorer complete: {len(state.get('views_created', []))} views created, "
                f"{len(state.get('existing_views', []))} views found"
            )
        else:
            state['error'] = f"Explorer failed: {result.get('message', 'Unknown error')}"
            logger.error(state['error'])

    except Exception as e:
        state['error'] = f"Explorer error: {str(e)}"
        logger.error(f"Explorer failed with exception: {e}", exc_info=True)

    logger.info("EXPLORE NODE - Complete")
    logger.info("=" * 80)
    return state


def research_node(state: ResearchState) -> ResearchState:
    """
    Run Researcher agent to create Layer 2 research views and analyze data.

    Args:
        state: Current state

    Returns:
        Updated state with Researcher results
    """
    logger.info("=" * 80)
    logger.info("RESEARCH NODE - Starting")
    logger.info("=" * 80)
    state['current_step'] = "RESEARCH"

    try:
        # Initialize Researcher
        db = get_db()
        researcher = ResearcherAgent(db=db, role=state['user_role'])
        logger.info(f"Researcher initialized for role: {state['user_role']}")

        # Build context from Explorer (if available)
        context = None
        if state.get('explorer_result'):
            context = state['explorer_result'].get('context')
            logger.info(f"Using Explorer context with keys: {list(context.keys()) if context else 'None'}")

        # Process query
        logger.info(f"Processing query: {state['user_query']}")
        result = researcher.process(state['user_query'], context=context)
        logger.info(f"Researcher process returned: success={result.get('success')}")

        # Update state
        state['researcher_result'] = result

        if result['success']:
            # Accumulate query results
            if 'query_results' in result:
                state['query_results'] = result['query_results']
                logger.info(f"Query results count: {len(result['query_results'])}")

            # Accumulate created views
            if 'created_views' in result:
                created_view_names = [v.view_name for v in result.get('created_views', [])]
                logger.info(f"Views created by Researcher: {created_view_names}")
                logger.info(f"Current views_created list before extend: {state.get('views_created', [])}")
                state['views_created'].extend(created_view_names)
                logger.info(f"Current views_created list after extend: {state['views_created']}")

            # Store analysis
            state['analysis'] = result.get('analysis', '')

            logger.info(
                f"Researcher complete: {len(result.get('query_results', []))} queries executed, "
                f"{len(result.get('created_views', []))} views created"
            )
        else:
            state['error'] = f"Researcher failed: {result.get('message', 'Unknown error')}"
            logger.error(state['error'])

    except Exception as e:
        state['error'] = f"Researcher error: {str(e)}"
        logger.error(f"Researcher failed with exception: {e}", exc_info=True)

    logger.info("RESEARCH NODE - Complete")
    logger.info("=" * 80)
    return state


def report_node(state: ResearchState) -> ResearchState:
    """
    Generate final research report from Explorer and Researcher results.

    Args:
        state: Current state

    Returns:
        Updated state with final report
    """
    logger.info("Generating final report...")
    state['current_step'] = "REPORT"

    try:
        report_sections = []

        # Header
        report_sections.append("=" * 80)
        report_sections.append(f"RESEARCH REPORT: {state['session_id']}")
        report_sections.append("=" * 80)
        report_sections.append(f"\nQuery: {state['user_query']}\n")

        # Error handling
        if state.get('error'):
            report_sections.append("\n⚠️  WARNING: Workflow encountered errors\n")
            report_sections.append(f"Error: {state['error']}\n")
            report_sections.append("Partial results below:\n")

        # Explorer findings
        if state.get('explorer_result'):
            report_sections.append("\n## EXPLORATION PHASE")
            report_sections.append("-" * 80)

            relevant_tables = state.get('relevant_tables', [])
            if relevant_tables:
                report_sections.append(f"\nRelevant Tables: {', '.join(relevant_tables)}")

            existing_views = state.get('existing_views', [])
            if existing_views:
                report_sections.append(f"Existing Views Found: {len(existing_views)}")
                for view_name in existing_views[:5]:  # Show first 5
                    report_sections.append(f"  - {view_name}")

        # Researcher findings
        if state.get('researcher_result'):
            report_sections.append("\n\n## ANALYSIS PHASE")
            report_sections.append("-" * 80)

            analysis = state.get('analysis', '')
            if analysis:
                report_sections.append(f"\n{analysis}")

            query_results = state.get('query_results', [])
            if query_results:
                successful_queries = [qr for qr in query_results if qr.get('success')]
                report_sections.append(f"\n\nAnalytical Queries Executed: {len(successful_queries)} successful, {len(query_results) - len(successful_queries)} failed")

                # Show first 5 successful queries with actual results
                for qr in successful_queries[:5]:
                    report_sections.append(f"\n  • {qr.get('purpose', 'Unknown')}")
                    report_sections.append(f"    Rows: {qr.get('row_count', 0)}")

                    # Show sample data from results
                    if qr.get('results') and len(qr['results']) > 0:
                        sample = qr['results'][0]  # First row
                        report_sections.append(f"    Sample: {dict(sample)}")

        # Views created
        views_created = state.get('views_created', [])
        logger.info(f"REPORT: views_created list = {views_created}")
        logger.info(f"REPORT: Unique views = {list(set(views_created)) if views_created else []}")

        if views_created:
            # Deduplicate views for display
            unique_views = list(dict.fromkeys(views_created))  # Preserves order
            report_sections.append("\n\n## VIEWS CREATED")
            report_sections.append("-" * 80)
            report_sections.append(f"\nTotal Views: {len(unique_views)}")
            for view_name in unique_views:
                report_sections.append(f"  ✓ {view_name}")

        # Final report from researcher
        if state.get('researcher_result'):
            researcher_report = state['researcher_result'].get('report', '')
            if researcher_report:
                report_sections.append("\n\n## FINDINGS")
                report_sections.append("-" * 80)
                report_sections.append(f"\n{researcher_report}")

        # Footer
        report_sections.append("\n" + "=" * 80)
        report_sections.append(f"Session: {state['session_id']}")
        report_sections.append(f"Started: {state.get('start_time', 'Unknown')}")
        report_sections.append(f"Completed: {datetime.utcnow().isoformat()}")
        report_sections.append("=" * 80)

        # Combine report
        state['final_report'] = "\n".join(report_sections)
        state['end_time'] = datetime.utcnow().isoformat()

        logger.info("Report generated successfully")

    except Exception as e:
        state['error'] = f"Report generation error: {str(e)}"
        logger.error(f"Report generation failed: {e}", exc_info=True)

        # Fallback minimal report
        state['final_report'] = f"Error generating report: {e}"

    return state


def build_research_graph() -> StateGraph:
    """
    Build the LangGraph StateGraph for research workflow.

    Returns:
        Compiled StateGraph
    """
    logger.info("Building research workflow graph...")

    # Create graph
    workflow = StateGraph(ResearchState)

    # Add nodes
    workflow.add_node("start", start_node)
    workflow.add_node("explore", explore_node)
    workflow.add_node("research", research_node)
    workflow.add_node("report", report_node)

    # Set entry point
    workflow.set_entry_point("start")

    # Add edges
    # START → EXPLORE (always for MVP)
    workflow.add_edge("start", "explore")

    # EXPLORE → RESEARCH (always)
    workflow.add_edge("explore", "research")

    # RESEARCH → REPORT (always)
    workflow.add_edge("research", "report")

    # REPORT → END
    workflow.add_edge("report", END)

    # Compile
    app = workflow.compile()

    logger.info("Research workflow graph built successfully")

    return app


def run_research_workflow(
    user_query: str,
    user_role: str = "fraud_analyst",
    session_id: str = None
) -> Dict[str, Any]:
    """
    Execute the full research workflow for a user query.

    Args:
        user_query: User's natural language query
        user_role: User's role for security context
        session_id: Optional session ID

    Returns:
        Dict with final state and report
    """
    logger.info(f"Starting research workflow for query: {user_query}")

    try:
        # Initialize database connection (ensure singleton is initialized)
        from pathlib import Path
        db_path = Path(__file__).parent.parent.parent / 'data' / 'researchdb.db'
        get_db(str(db_path))

        # Create initial state
        initial_state = create_initial_state(
            user_query=user_query,
            user_role=user_role,
            session_id=session_id
        )

        # Build and run graph
        app = build_research_graph()
        final_state = app.invoke(initial_state)

        # Log summary
        logger.info("Workflow complete!")
        logger.info(format_state_summary(final_state))

        return {
            'success': not bool(final_state.get('error')),
            'session_id': final_state['session_id'],
            'report': final_state.get('final_report', ''),
            'views_created': final_state.get('views_created', []),
            'views_used': final_state.get('existing_views', []),
            'error': final_state.get('error'),
            'state': final_state
        }

    except Exception as e:
        logger.error(f"Workflow failed: {e}", exc_info=True)
        return {
            'success': False,
            'error': str(e),
            'report': f"Workflow failed with error: {e}"
        }
