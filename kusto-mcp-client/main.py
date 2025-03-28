import asyncio
import os
import json

from dotenv import load_dotenv
from typing import Optional, Dict, List
from contextlib import AsyncExitStack

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from openai import AzureOpenAI

load_dotenv() # take environment variables from .env.

class MCPClient():
    def __init__(self):
        self.session: Optional[ClientSession] = None
        self.exit_stack = AsyncExitStack()
        self.openai = AzureOpenAI(
            api_version=os.getenv("AZUREOPENAI_API_VERSION"),
            api_key=os.getenv("AZUREOPENAI_API_KEY"),
            azure_endpoint=os.getenv("AZUREOPENAI_ENDPOINT")
        )
        self.conversation_context: List[dict] = []
        self.active_tools: Dict[str, dict] = {}
    
    def _manage_context(self) -> List[Dict]:
        return self.conversation_context
    
    async def connect_to_server(self, server_script_path: str):
        """Connect to an MCP server

        Args:
            server_script_path: Path to the server script (.py or .js)
        """
        server_params = StdioServerParameters(
            command="python",
            args=[server_script_path],
            env=None
        )

        stdio_transport = await self.exit_stack.enter_async_context(stdio_client(server_params))
        self.stdio, self.write = stdio_transport
        self.session = await self.exit_stack.enter_async_context(ClientSession(self.stdio, self.write))

        await self.session.initialize()

        # List available tools
        response = await self.session.list_tools()
        print("\nConnected to server with tools:", [tool.name for tool in response.tools])

    async def process_query(self, query: str):
        """Process a query using AzureOpenAI and available tools"""
        # Add user message to context
        self.conversation_context.append({"role": "user", "content": query})

        # Get available tools from server
        tools_response = await self.session.list_tools()
        available_tools = [{
            "type": "function",
            "function": {
                "name": t.name,
                "description": t.description,
                "parameters": t.inputSchema
            }
        } for t in tools_response.tools]

        # Initial OpenAI call
        response = self.openai.chat.completions.create(
            model=os.getenv("AZUREOPENAI_MODEL"),
            messages=self._manage_context(),
            tools=available_tools,
            tool_choice="auto",
        )
        message = response.choices[0].message
        self.conversation_context.append(message)

        if message.tool_calls:
            tools_responses = []

            # Call each tool requested by LLM and get its output
            for tool_call in message.tool_calls:
                tool_name = tool_call.function.name
                tool_args = json.loads(tool_call.function.arguments)

                # Call tool on MCP server
                tool_result = await self.session.call_tool(tool_name, tool_args)
                text_content = " ".join([item.text for item in tool_result.content if hasattr(item, 'text')])
                tools_responses.append({
                    "role": "tool",
                    "tool_call_id": tool_call.id,
                    "content": text_content
                })
            
            self.conversation_context.extend(tools_responses)

            # Get follow-up response with tool results
            response = self.openai.chat.completions.create(
                model=os.getenv("AZUREOPENAI_MODEL"),
                messages=self._manage_context(),
                tools=available_tools,
                tool_choice="auto",
            )

            self.conversation_context.append({
                "role": "assistant",
                "content": response.choices[0].message.content
            })
            return response.choices[0].message.content
        else:
            self.conversation_context.append(message.content)
            return message.content

    async def chat_loop(self):
        """Run an interactive chat loop"""
        print("\nMCP Client Started!")
        print("Type your queries or 'quit' to exit.")

        while True:
            try:
                query = input("\nQuery: ").strip()
                if query.lower() == 'quit':
                    break

                if query.lower() == 'reset':
                    self.conversation_context = []
                    print("\nConversation context reset.")
                    continue

                response = await self.process_query(query)
                print("\n" + response)

            except Exception as e:
                print(f"\nError: {str(e)}")

    async def cleanup(self):
        """Clean up resources"""
        await self.exit_stack.aclose()

async def main():
    if len(sys.argv) < 2:
        print("Usage: python main.py <url>")
        sys.exit(1)
    
    client = MCPClient()
    try:
        await client.connect_to_server(sys.argv[1])
        await client.chat_loop()
    finally:
        await client.cleanup()

if __name__ == "__main__":
    import sys
    asyncio.run(main())