import json
import os
import sqlite3
import hashlib
import hmac
import secrets
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_PATH = os.path.join(DATA_DIR, "app.db")


def ensure_db():
    os.makedirs(DATA_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          username_key TEXT UNIQUE NOT NULL,
          username_display TEXT NOT NULL,
          salt TEXT NOT NULL,
          password_hash TEXT NOT NULL,
          created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS user_data (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          username_key TEXT NOT NULL,
          data_key TEXT NOT NULL,
          value_json TEXT NOT NULL,
          updated_at TEXT NOT NULL DEFAULT (datetime('now')),
          UNIQUE(username_key, data_key)
        )
        """
    )
    conn.commit()
    conn.close()


def normalize_username(value: str) -> str:
    return (value or "").strip().lower()


def hash_password(password: str, salt_hex: str) -> str:
    salt = bytes.fromhex(salt_hex)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 120000)
    return digest.hex()


class AppHandler(SimpleHTTPRequestHandler):
    def _send_json(self, status: int, payload):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self):
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length > 0 else b"{}"
        try:
            return json.loads(raw.decode("utf-8"))
        except Exception:
            return {}

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, PUT, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/api/auth/exists":
            query = parse_qs(parsed.query)
            username_key = normalize_username((query.get("username") or [""])[0])
            conn = sqlite3.connect(DB_PATH)
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM users WHERE username_key = ?", (username_key,))
            exists = cur.fetchone() is not None
            conn.close()
            return self._send_json(200, {"ok": True, "exists": exists})

        if parsed.path == "/api/user-data/all":
            query = parse_qs(parsed.query)
            username_key = normalize_username((query.get("username") or [""])[0])
            if not username_key:
                return self._send_json(400, {"ok": False, "error": "username_required"})
            conn = sqlite3.connect(DB_PATH)
            cur = conn.cursor()
            cur.execute("SELECT data_key, value_json FROM user_data WHERE username_key = ?", (username_key,))
            rows = cur.fetchall()
            conn.close()
            out = {}
            for key, value_json in rows:
                try:
                    out[key] = json.loads(value_json)
                except Exception:
                    out[key] = None
            return self._send_json(200, {"ok": True, "data": out})

        return super().do_GET()

    def do_POST(self):
        parsed = urlparse(self.path)
        payload = self._read_json()

        if parsed.path == "/api/auth/register":
            username_raw = (payload.get("username") or "").strip()
            password = payload.get("password") or ""
            username_key = normalize_username(username_raw)
            if len(username_key) < 3 or len(password) < 6:
                return self._send_json(400, {"ok": False, "error": "invalid_credentials"})

            conn = sqlite3.connect(DB_PATH)
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM users WHERE username_key = ?", (username_key,))
            if cur.fetchone():
                conn.close()
                return self._send_json(409, {"ok": False, "error": "user_exists"})

            salt_hex = secrets.token_hex(16)
            pw_hash = hash_password(password, salt_hex)
            cur.execute(
                "INSERT INTO users (username_key, username_display, salt, password_hash) VALUES (?, ?, ?, ?)",
                (username_key, username_raw, salt_hex, pw_hash),
            )
            conn.commit()
            conn.close()
            return self._send_json(200, {"ok": True, "usernameKey": username_key, "username": username_raw})

        if parsed.path == "/api/auth/login":
            username = normalize_username(payload.get("username") or "")
            password = payload.get("password") or ""
            conn = sqlite3.connect(DB_PATH)
            cur = conn.cursor()
            cur.execute(
                "SELECT username_key, username_display, salt, password_hash FROM users WHERE username_key = ?",
                (username,),
            )
            row = cur.fetchone()
            conn.close()
            if not row:
                return self._send_json(404, {"ok": False, "error": "not_found"})
            username_key, username_display, salt_hex, stored_hash = row
            given_hash = hash_password(password, salt_hex)
            if not hmac.compare_digest(given_hash, stored_hash):
                return self._send_json(401, {"ok": False, "error": "invalid_password"})
            return self._send_json(200, {"ok": True, "usernameKey": username_key, "username": username_display})

        return self._send_json(404, {"ok": False, "error": "not_found"})

    def do_PUT(self):
        parsed = urlparse(self.path)
        if parsed.path != "/api/user-data":
            return self._send_json(404, {"ok": False, "error": "not_found"})
        payload = self._read_json()
        username_key = normalize_username(payload.get("username") or "")
        data_key = (payload.get("key") or "").strip()
        if not username_key or not data_key:
            return self._send_json(400, {"ok": False, "error": "invalid_payload"})
        value_json = json.dumps(payload.get("value"), ensure_ascii=False)
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO user_data (username_key, data_key, value_json, updated_at)
            VALUES (?, ?, ?, datetime('now'))
            ON CONFLICT(username_key, data_key)
            DO UPDATE SET value_json = excluded.value_json, updated_at = datetime('now')
            """,
            (username_key, data_key, value_json),
        )
        conn.commit()
        conn.close()
        return self._send_json(200, {"ok": True})


def run():
    ensure_db()
    port = 5500
    httpd = ThreadingHTTPServer(("127.0.0.1", port), AppHandler)
    print(f"WB server started: http://127.0.0.1:{port}")
    httpd.serve_forever()


if __name__ == "__main__":
    run()

