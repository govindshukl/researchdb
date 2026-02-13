"""
Claude API client wrapper with retry logic and token tracking.
"""

import logging
import time
import os
from typing import List, Dict, Any, Optional, Generator
from anthropic import Anthropic, APIError, RateLimitError
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)


class ClaudeClient:
    """
    Wrapper for Claude API with retry logic and token tracking.
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: str = "claude-haiku-4-5-20251001",
        max_tokens: int = 4096,
        temperature: float = 0.7
    ):
        """
        Initialize Claude API client.

        Args:
            api_key: Anthropic API key (uses env var if None)
            model: Model name
            max_tokens: Maximum tokens in response
            temperature: Sampling temperature (0-1)
        """
        self.api_key = api_key or os.getenv("ANTHROPIC_API_KEY")
        if not self.api_key:
            raise ValueError("ANTHROPIC_API_KEY not set. Set it in .env file or pass as parameter.")

        self.client = Anthropic(api_key=self.api_key)
        self.model = model
        self.max_tokens = max_tokens
        self.temperature = temperature

        # Token tracking
        self.total_input_tokens = 0
        self.total_output_tokens = 0
        self.total_cost = 0.0

        logger.info(f"Claude client initialized: {model}")

    def chat_completion(
        self,
        messages: List[Dict[str, str]],
        system_prompt: Optional[str] = None,
        max_retries: int = 3,
        retry_delay: float = 2.0,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Send a chat completion request with retry logic.

        Args:
            messages: List of message dicts with 'role' and 'content'
            system_prompt: Optional system prompt
            max_retries: Maximum number of retry attempts
            retry_delay: Delay between retries (seconds)
            **kwargs: Additional parameters for API call

        Returns:
            Dict with response data:
                - content: Response text
                - usage: Token usage stats
                - model: Model used
        """
        attempt = 0
        last_error = None

        while attempt < max_retries:
            try:
                # Build API call parameters
                api_params = {
                    "model": kwargs.get("model", self.model),
                    "max_tokens": kwargs.get("max_tokens", self.max_tokens),
                    "temperature": kwargs.get("temperature", self.temperature),
                    "messages": messages
                }

                if system_prompt:
                    api_params["system"] = system_prompt

                # Make API call
                logger.debug(f"Calling Claude API (attempt {attempt + 1}/{max_retries})...")
                response = self.client.messages.create(**api_params)

                # Extract response
                content = response.content[0].text if response.content else ""

                # Track usage
                if hasattr(response, 'usage'):
                    self.total_input_tokens += response.usage.input_tokens
                    self.total_output_tokens += response.usage.output_tokens

                    # Estimate cost (approximate rates for Claude Sonnet)
                    input_cost = response.usage.input_tokens * 0.003 / 1000  # $3 per 1M tokens
                    output_cost = response.usage.output_tokens * 0.015 / 1000  # $15 per 1M tokens
                    call_cost = input_cost + output_cost
                    self.total_cost += call_cost

                    logger.info(
                        f"API call successful: "
                        f"{response.usage.input_tokens} in / {response.usage.output_tokens} out "
                        f"(cost: ${call_cost:.4f})"
                    )

                return {
                    "content": content,
                    "usage": {
                        "input_tokens": response.usage.input_tokens if hasattr(response, 'usage') else 0,
                        "output_tokens": response.usage.output_tokens if hasattr(response, 'usage') else 0
                    },
                    "model": response.model,
                    "stop_reason": response.stop_reason
                }

            except RateLimitError as e:
                last_error = e
                wait_time = retry_delay * (2 ** attempt)  # Exponential backoff
                logger.warning(f"Rate limit hit, waiting {wait_time}s before retry...")
                time.sleep(wait_time)
                attempt += 1

            except APIError as e:
                last_error = e
                logger.error(f"API error: {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    attempt += 1
                else:
                    raise

            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                raise

        # If we get here, all retries failed
        raise RuntimeError(f"Failed after {max_retries} attempts. Last error: {last_error}")

    def streaming_completion(
        self,
        messages: List[Dict[str, str]],
        system_prompt: Optional[str] = None,
        **kwargs
    ) -> Generator[str, None, None]:
        """
        Stream a chat completion response.

        Args:
            messages: List of message dicts
            system_prompt: Optional system prompt
            **kwargs: Additional parameters

        Yields:
            Text chunks as they arrive
        """
        api_params = {
            "model": kwargs.get("model", self.model),
            "max_tokens": kwargs.get("max_tokens", self.max_tokens),
            "temperature": kwargs.get("temperature", self.temperature),
            "messages": messages,
            "stream": True
        }

        if system_prompt:
            api_params["system"] = system_prompt

        logger.debug("Starting streaming completion...")

        with self.client.messages.stream(**api_params) as stream:
            for text in stream.text_stream:
                yield text

            # Track final usage
            final_message = stream.get_final_message()
            if hasattr(final_message, 'usage'):
                self.total_input_tokens += final_message.usage.input_tokens
                self.total_output_tokens += final_message.usage.output_tokens

    def get_usage_stats(self) -> Dict[str, Any]:
        """
        Get cumulative usage statistics.

        Returns:
            Dict with usage stats
        """
        return {
            "total_input_tokens": self.total_input_tokens,
            "total_output_tokens": self.total_output_tokens,
            "total_tokens": self.total_input_tokens + self.total_output_tokens,
            "total_cost": round(self.total_cost, 4),
            "model": self.model
        }

    def reset_usage_stats(self):
        """Reset usage counters."""
        self.total_input_tokens = 0
        self.total_output_tokens = 0
        self.total_cost = 0.0
        logger.info("Usage stats reset")

    def format_messages(
        self,
        user_message: str,
        assistant_messages: Optional[List[str]] = None,
        context: Optional[str] = None
    ) -> List[Dict[str, str]]:
        """
        Helper to format messages for API call.

        Args:
            user_message: User's message
            assistant_messages: Previous assistant responses (optional)
            context: Additional context to prepend (optional)

        Returns:
            Formatted messages list
        """
        messages = []

        # Add context as initial user message if provided
        if context:
            messages.append({
                "role": "user",
                "content": f"Context:\n{context}"
            })
            messages.append({
                "role": "assistant",
                "content": "I've reviewed the context and I'm ready to help."
            })

        # Add conversation history if provided
        if assistant_messages:
            for i, asst_msg in enumerate(assistant_messages):
                if i > 0:  # Skip first as it's in context
                    messages.append({
                        "role": "assistant",
                        "content": asst_msg
                    })

        # Add current user message
        messages.append({
            "role": "user",
            "content": user_message
        })

        return messages


# Singleton instance
_client_instance: Optional[ClaudeClient] = None


def get_claude_client(
    api_key: Optional[str] = None,
    model: str = "claude-haiku-4-5-20251001"
) -> ClaudeClient:
    """
    Get global Claude client instance.

    Args:
        api_key: API key (optional, uses env var)
        model: Model name

    Returns:
        ClaudeClient instance
    """
    global _client_instance
    if _client_instance is None:
        _client_instance = ClaudeClient(api_key=api_key, model=model)
    return _client_instance
