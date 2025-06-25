import os
from dotenv import load_dotenv
from typing import Optional
from openai import OpenAI

class OpenRouterClient:
    def __init__(self, model: Optional[str] = None):

        load_dotenv()

        """
        Initialize OpenRouter client with environment configuration.

        Args:
            model: Optional model override (e.g. "anthropic/claude-3-opus")
        """

        self.api_key = os.getenv("OPENROUTER_API_KEY")
        self.base_url = os.getenv("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1")
        self.model = model or os.getenv("OPENROUTER_DEFAULT_MODEL")

        if not self.api_key:
            raise ValueError("OPENROUTER_API_KEY not found in environment variables")

        self.client = OpenAI(
            api_key=self.api_key,
            base_url=self.base_url
        )

    def chat_completion(self, messages: list[dict], **kwargs):
        """
        Generic chat completion interface.

        Args:
            messages: List of message dicts (role/content)
            **kwargs: Additional OpenAI API params

        Returns:
            OpenAI completion response
        """
        return self.client.chat.completions.create(
            model=self.model,
            messages=messages,
            **kwargs
        )

    def quick_query(self, prompt: str, system_message: str = None, **kwargs):
        """
        Simplified interface for single-prompt queries.

        Args:
            prompt: User prompt text
            system_message: Optional system message
            **kwargs: Additional OpenAI API params

        Returns:
            Generated text response
        """
        messages = []
        if system_message:
            messages.append({"role": "system", "content": system_message})
        messages.append({"role": "user", "content": prompt})

        response = self.chat_completion(messages, **kwargs)
        return response.choices[0].message.content


###############################################################################
# CLI / quick demo when executed as a script
###############################################################################

if __name__ == "__main__":
    client = OpenRouterClient()
    response = client.quick_query(
        "What's 2+2?",
        system_message="You are a helpful math assistant. Respond concisely."
    )
    print("\n=== Response ===")
    print(response)