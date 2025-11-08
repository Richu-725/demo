import os
import requests

def main() -> None:
    api_key = os.environ.get("OE_API_KEY")
    if not api_key:
        raise SystemExit("OE_API_KEY not set")
    headers = {"Authorization": f"Bearer {api_key}", "Accept": "application/json"}
    resp = requests.get(
        "https://api.openelectricity.org.au/v1/facilities/",
        headers=headers,
        params={"network_id": ["NEM"]},
        timeout=30,
    )
    print(resp.status_code)
    print(resp.text[:400])

if __name__ == "__main__":
    main()
