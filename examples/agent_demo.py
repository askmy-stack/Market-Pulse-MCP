"""Example: query MarketPulse API from an agent script."""

import httpx

BASE = "http://localhost:8000"


def main():
    with httpx.Client() as client:
        symbols = client.get(f"{BASE}/symbols").json()
        print("Symbols:", symbols)

        explain = client.get(f"{BASE}/explain/AAPL").json()
        print("\nExplain AAPL:")
        print(explain.get("explanation"))
        print("\nDisclaimer:", explain.get("disclaimer"))

        brief = client.post(f"{BASE}/brief/market").json()
        print("\nMarket Brief:")
        print(brief.get("content"))


if __name__ == "__main__":
    main()
