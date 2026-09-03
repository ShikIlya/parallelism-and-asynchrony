import aiohttp
from urllib.parse import urlparse

class RobotsParser:
    def __init__(self, session: aiohttp.ClientSession):
        self._session = session
        self._cache: dict[str, dict] = {}
        self._current_rules: dict = {}

    async def fetch_robots(self, base_url: str) -> dict:
        base_url = base_url.rstrip("/")

        if base_url in self._cache:
            self._current_rules = self._cache[base_url]

            return self._current_rules

        robots_url = base_url + "/robots.txt"

        try:
            async with self._session.get(robots_url) as response:
                if response.status != 200:
                    rules = {}
                else:
                    text = await response.text()
                    rules = self._parse_robots_text(text)
        except Exception:
            rules = {}

        self._cache[base_url] = rules
        self._current_rules = rules

        return rules

    def _get_agent_rules(
        self,
        rules: dict,
        user_agent: str,
    ) -> dict | None:
        normalized_agent = user_agent.lower()

        product_token = normalized_agent.split(
            "/",
            1,
        )[0]

        matching_agents = [
            agent
            for agent in rules
            if agent != "*"
            and product_token.startswith(agent)
        ]

        if matching_agents:
            most_specific_agent = max(
                matching_agents,
                key=len,
            )

            return rules[most_specific_agent]

        return rules.get("*")

    def can_fetch(self, url: str, user_agent: str = "*") -> bool:
        base_url = self._get_base_url(url)
        rules = self._cache.get(base_url)

        if rules is None:
            return True

        agent_rules = self._get_agent_rules(
            rules,
            user_agent,
        )

        if agent_rules is None:
            return True

        path = urlparse(url).path

        for disallow in agent_rules.get("disallow", []):
            if disallow and path.startswith(disallow):
                return False

        return True

    def get_crawl_delay(self, user_agent: str = "*") -> float:
        agent_rules = self._get_agent_rules(
            self._current_rules,
            user_agent,
        )

        if agent_rules is None:
            return 0.0

        return agent_rules.get(
            "crawl_delay",
            0.0,
        )

    def _get_base_url(self, url: str) -> str:
        parsed = urlparse(url)
        base = f"{parsed.scheme}://{parsed.netloc}"
        return base.rstrip("/")

    def _parse_robots_text(self, text: str) -> dict:
        rules = {}
        current_agents: list[str] = []
        has_rules = False

        for line in text.splitlines():
            line = line.strip()

            if not line or line.startswith("#"):
                continue

            if ":" not in line:
                continue

            key, value = line.split(":", 1)
            key = key.strip().lower()
            value = value.strip()

            if key == "user-agent":
                agent = value.lower()

                if has_rules:
                    current_agents = []
                    has_rules = False

                current_agents.append(agent)

                if agent not in rules:
                    rules[agent] = {"disallow": [], "crawl_delay": 0.0}

            elif key == "disallow" and current_agents:
                for agent in current_agents:
                    rules[agent]["disallow"].append(value)

                has_rules = True

            elif key == "crawl-delay" and current_agents:
                for agent in current_agents:
                    try:
                        rules[agent]["crawl_delay"] = float(value)
                    except ValueError:
                        pass

                has_rules = True

        return rules