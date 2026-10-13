"""The User-Agent the third-party sources ask for: "a descriptive User-Agent with a
maintainer or project URL" (zKillboard's API documentation, design §11a, §11b)."""

from evedw import __version__

PROJECT_URL = "https://github.com/wims80/eve-data-warehouse"


def user_agent(contact: str | None) -> str:
    who = f"; {contact}" if contact else ""
    return f"eve-data-warehouse/{__version__} (+{PROJECT_URL}{who})"
