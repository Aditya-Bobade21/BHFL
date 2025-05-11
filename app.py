from fastapi import FastAPI
import httpx
import sys

# ─── CONFIG ────────────────────────────────────────────────────────────────
API_REG_ENDPOINT = "https://bfhldevapigw.healthrx.co.in/hiring/generateWebhook/PYTHON"  # Registration endpoint
NAME             = "Aditya Bobade"                                             # Your name
REG_NO           = "0827CI221011"                                              # Your registration no.
EMAIL            = "adityabobade220721@acropolis.in"                           # Your email

SQL_QUERY = """
SELECT
  p.amount AS SALARY,
  CONCAT(e.first_name, ' ', e.last_name) AS NAME,
  TIMESTAMPDIFF(YEAR, e.dob, CURDATE()) AS AGE,
  d.department_name AS DEPARTMENT_NAME
FROM payments p
JOIN employee e ON p.emp_id = e.emp_id
JOIN department d ON e.department = d.department_id
WHERE DAY(p.payment_time) <> 1
  AND p.amount = (
    SELECT MAX(amount)
    FROM payments
    WHERE DAY(payment_time) <> 1
  );
""".strip()
# ────────────────────────────────────────────────────────────────────────────

app = FastAPI()

@app.on_event("startup")
async def startup_flow():
    async with httpx.AsyncClient() as client:
        # 1) Register / authenticate
        reg_payload = {"name": NAME, "regNo": REG_NO, "email": EMAIL}
        print(f"[DEBUG] → POST {API_REG_ENDPOINT} payload={reg_payload}")
        try:
            resp = await client.post(API_REG_ENDPOINT, json=reg_payload)
            print(f"[DEBUG] ← {resp.status_code} {resp.text}")
            resp.raise_for_status()
        except httpx.RequestError as e:
            print(f"[ERROR] Registration request failed: {e}", file=sys.stderr)
            sys.exit(1)
        except httpx.HTTPStatusError as e:
            print(f"[ERROR] Registration failed with status {e.response.status_code}: {e.response.text}", file=sys.stderr)
            sys.exit(1)

        data    = resp.json()
        webhook = data.get("webhook")
        token   = data.get("accessToken")
        if not webhook or not token:
            print(f"[ERROR] Missing webhook or accessToken in response: {data}", file=sys.stderr)
            sys.exit(1)

        print(f"[INFO] Received webhook: {webhook}")
        print(f"[INFO] Received token (truncated): {token[:8]}…")

        # 2) Submit final SQL query to the returned webhook URL
        headers = {
            "Authorization": f"{token}",
            "Content-Type":  "application/json"
        }
        final_payload = {"finalQuery": SQL_QUERY}

        print(f"[DEBUG] → POST {webhook} headers={headers} payload={{'finalQuery': SQL_QUERY[:30] + '...'}}")
        try:
            resp2 = await client.post(webhook, json=final_payload, headers=headers)
            print(f"[DEBUG] ← {resp2.status_code} {resp2.text}")
            resp2.raise_for_status()
        except httpx.RequestError as e:
            print(f"[ERROR] Submission request failed: {e}", file=sys.stderr)
            sys.exit(1)
        except httpx.HTTPStatusError as e:
            print(f"[ERROR] Submission failed with status {e.response.status_code}: {e.response.text}", file=sys.stderr)
            sys.exit(1)

        print("[SUCCESS] SQL query submitted successfully!")

@app.get("/")
async def root():
    return {"status": "running"}
