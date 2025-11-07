import os
import sys
import requests

def main() -> None:
    api_key = os.environ.get("OE_API_KEY")
    if not api_key:
        print("Missing OE_API_KEY", file=sys.stderr)
        sys.exit(1)
    print("API key repr:", repr(api_key))
    headers = {"Authorization": f"Bearer {api_key}"}
    params = {"network_id": ["NEM"]}
    resp = requests.get("https://api.openelectricity.org.au/v1/facilities/", headers=headers, params=params, timeout=30)
    print("Status:", resp.status_code)
    try:
        data = resp.json()
    except Exception:
        print("Raw text:", resp.text[:200])
    else:
        print("JSON keys:", list(data.keys()))
        if "error" in data:
            print("Error field:", data["error"])

if __name__ == "__main__":
    main()
