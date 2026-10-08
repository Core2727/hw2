from mcp.server.fastmcp import FastMCP

mcp = FastMCP("echo")

@mcp.tool()
def ping() -> str:
    """reply pong"""
    return "pong"

if __name__ == "__main__":
    mcp.run()
