import json
import os
import sqlite3
import uuid
from datetime import date, datetime, timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse


BASE_DIR = Path(__file__).resolve().parent
HTML_PATH = BASE_DIR / "base.html"
DB_PATH = os.environ.get("DB_PATH", str(BASE_DIR / "assignments.db"))
MAX_CONTENT_LENGTH = 1_000_000
BACKUP_PASSWORD = os.environ.get("BACKUP_PASSWORD", "123123")


class PayloadTooLargeError(Exception):
    pass


class AssignmentStore:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        with self._get_connection() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS assignments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    masechet TEXT NOT NULL,
                    daf INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    dedication TEXT,
                    learned INTEGER NOT NULL DEFAULT 0,
                    is_full_masechet INTEGER NOT NULL DEFAULT 0,
                    UNIQUE (masechet, daf)
                )
                """
            )

    def list_assignments(self) -> list[dict]:
        with self._get_connection() as connection:
            cursor = connection.execute(
                "SELECT id, masechet, daf, name, dedication, learned, is_full_masechet FROM assignments "
                "ORDER BY masechet, daf"
            )
            rows = cursor.fetchall()
        return [self._row_to_dict(row) for row in rows]

    def get_assignment(self, assignment_id: int) -> dict | None:
        with self._get_connection() as connection:
            cursor = connection.execute(
                "SELECT id, masechet, daf, name, dedication, learned, is_full_masechet FROM assignments WHERE id = ?",
                (assignment_id,),
            )
            row = cursor.fetchone()
        return self._row_to_dict(row) if row else None

    def create_assignment(self, payload: dict) -> dict:
        records = self.create_assignments([payload])
        return records[0]

    def create_assignments(self, payloads: list[dict]) -> list[dict]:
        if not payloads or not isinstance(payloads, list):
            raise ValueError("Missing required fields")
        records = [self._parse_payload(payload, require_fields=True) for payload in payloads]
        assignment_ids = []
        with self._get_connection() as connection:
            for record in records:
                cursor = connection.execute(
                    """
                    INSERT INTO assignments (masechet, daf, name, dedication, learned, is_full_masechet)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        record["masechet"],
                        record["daf"],
                        record["name"],
                        record.get("dedication"),
                        int(record.get("learned", False)),
                        int(record.get("is_full_masechet", False)),
                    ),
                )
                assignment_ids.append(cursor.lastrowid)
        return self._get_assignments_by_ids(assignment_ids)

    def update_assignment(self, assignment_id: int, payload: dict) -> dict | None:
        existing = self.get_assignment(assignment_id)
        if not existing:
            return None
        record = self._parse_payload(payload, require_fields=False, defaults=existing)
        with self._get_connection() as connection:
            connection.execute(
                """
                UPDATE assignments
                SET masechet = ?, daf = ?, name = ?, dedication = ?, learned = ?, is_full_masechet = ?
                WHERE id = ?
                """,
                (
                    record["masechet"],
                    record["daf"],
                    record["name"],
                    record.get("dedication"),
                    int(record.get("learned", False)),
                    int(record.get("is_full_masechet", False)),
                    assignment_id,
                ),
            )
        return self.get_assignment(assignment_id)

    def delete_assignment(self, assignment_id: int) -> bool:
        with self._get_connection() as connection:
            cursor = connection.execute(
                "DELETE FROM assignments WHERE id = ?",
                (assignment_id,),
            )
        return cursor.rowcount > 0

    def replace_assignments(self, payloads: list[dict]) -> int:
        if payloads is None or not isinstance(payloads, list):
            raise ValueError("Missing required fields")
        records = [self._parse_payload(payload, require_fields=True) for payload in payloads]
        with self._get_connection() as connection:
            connection.execute("DELETE FROM assignments")
            for record in records:
                connection.execute(
                    """
                    INSERT INTO assignments (masechet, daf, name, dedication, learned, is_full_masechet)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        record["masechet"],
                        record["daf"],
                        record["name"],
                        record.get("dedication"),
                        int(record.get("learned", False)),
                        int(record.get("is_full_masechet", False)),
                    ),
                )
        return len(records)

    def _get_connection(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        return connection

    @staticmethod
    def _row_to_dict(row: sqlite3.Row | None) -> dict | None:
        if row is None:
            return None
        return {
            "id": row["id"],
            "masechet": row["masechet"],
            "daf": row["daf"],
            "name": row["name"],
            "dedication": row["dedication"] or "",
            "learned": bool(row["learned"]),
            "is_full_masechet": bool(row["is_full_masechet"]),
        }

    def _get_assignments_by_ids(self, assignment_ids: list[int]) -> list[dict]:
        if not assignment_ids:
            return []
        placeholders = ",".join("?" for _ in assignment_ids)
        with self._get_connection() as connection:
            cursor = connection.execute(
                f"""
                SELECT id, masechet, daf, name, dedication, learned, is_full_masechet
                FROM assignments
                WHERE id IN ({placeholders})
                ORDER BY id
                """,
                assignment_ids,
            )
            rows = cursor.fetchall()
        return [self._row_to_dict(row) for row in rows]

    @staticmethod
    def _parse_payload(
        payload: dict,
        *,
        require_fields: bool,
        defaults: dict | None = None,
    ) -> dict:
        defaults = defaults or {}
        masechet = (payload.get("masechet") if payload else None) or defaults.get("masechet")
        name = (payload.get("name") if payload else None) or defaults.get("name")
        daf = payload.get("daf") if payload else None
        if daf is None:
            daf = defaults.get("daf")

        masechet = str(masechet).strip() if masechet is not None else ""
        name = str(name).strip() if name is not None else ""
        if payload and "dedication" in payload:
            dedication = payload.get("dedication")
        else:
            dedication = defaults.get("dedication")
        if payload and "learned" in payload:
            learned = payload.get("learned")
        else:
            learned = defaults.get("learned", False)
        if payload and "is_full_masechet" in payload:
            is_full_masechet = payload.get("is_full_masechet")
        else:
            is_full_masechet = defaults.get("is_full_masechet", False)

        if require_fields and (not masechet or not name or daf is None):
            raise ValueError("Missing required fields")

        try:
            daf_value = int(daf)
        except (TypeError, ValueError) as error:
            raise ValueError("Invalid daf") from error

        return {
            "masechet": masechet,
            "name": name,
            "daf": daf_value,
            "dedication": str(dedication).strip() if dedication is not None else "",
            "learned": bool(learned),
            "is_full_masechet": bool(is_full_masechet),
        }


store = AssignmentStore(DB_PATH)


class RequestHandler(BaseHTTPRequestHandler):
    server_version = "DafHTTP/1.0"

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        path = unquote(parsed.path)
        if path in ("/", "/base.html", "/index.html"):
            self._serve_html()
            return
        if path == "/api/assignments":
            self._send_json(store.list_assignments())
            return
        if path == "/api/calendar/ics":
            self._serve_ics(parsed)
            return
        if path == "/dafdaf":
            self._serve_backup_page(parsed)
            return
        if path == "/dafdaf/export":
            self._serve_backup_export(parsed)
            return
        if path == "/יחד אחים.png":
            self._serve_file(BASE_DIR / "יחד אחים.png", "image/png")
            return
        self.send_error(404, "Not Found")

    def do_POST(self) -> None:
        if self.path.startswith("/dafdaf/import"):
            self._handle_backup_import()
            return
        if self.path != "/api/assignments":
            self.send_error(404, "Not Found")
            return
        try:
            payload = self._read_json()
        except PayloadTooLargeError:
            self._send_json({"error": "Payload too large"}, status=413)
            return
        if payload is None:
            self._send_json({"error": "Invalid JSON"}, status=400)
            return
        try:
            assignments_payload = payload.get("assignments")
            if assignments_payload is not None:
                if not isinstance(assignments_payload, list):
                    raise ValueError("Invalid assignments payload")
                record = store.create_assignments(assignments_payload)
            else:
                record = store.create_assignment(payload)
        except ValueError:
            self._send_json({"error": "Missing required fields"}, status=400)
            return
        except sqlite3.IntegrityError:
            self._send_json({"error": "Assignment already exists"}, status=409)
            return
        self._send_json(record, status=201)

    def do_PUT(self) -> None:
        if not self.path.startswith("/api/assignments/"):
            self.send_error(404, "Not Found")
            return
        assignment_id = self._extract_id()
        if assignment_id is None:
            self._send_json({"error": "Invalid assignment id"}, status=400)
            return
        try:
            payload = self._read_json()
        except PayloadTooLargeError:
            self._send_json({"error": "Payload too large"}, status=413)
            return
        if payload is None:
            self._send_json({"error": "Invalid JSON"}, status=400)
            return
        try:
            record = store.update_assignment(assignment_id, payload)
        except ValueError:
            self._send_json({"error": "Invalid payload"}, status=400)
            return
        except sqlite3.IntegrityError:
            self._send_json({"error": "Assignment already exists"}, status=409)
            return
        if record is None:
            self._send_json({"error": "Assignment not found"}, status=404)
            return
        self._send_json(record)

    def do_DELETE(self) -> None:
        if not self.path.startswith("/api/assignments/"):
            self.send_error(404, "Not Found")
            return
        assignment_id = self._extract_id()
        if assignment_id is None:
            self._send_json({"error": "Invalid assignment id"}, status=400)
            return
        deleted = store.delete_assignment(assignment_id)
        if not deleted:
            self._send_json({"error": "Assignment not found"}, status=404)
            return
        self._send_json({"ok": True})

    def _serve_html(self) -> None:
        if not HTML_PATH.exists():
            self.send_error(404, "Not Found")
            return
        content = HTML_PATH.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)

    def _serve_file(self, path: Path, content_type: str) -> None:
        if not path.exists() or not path.is_file():
            self.send_error(404, "Not Found")
            return
        content = path.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)

    def _serve_backup_page(self, parsed) -> None:
        params = parse_qs(parsed.query)
        if not self._is_backup_authorized(parsed):
            self._send_html(
                """
                <!DOCTYPE html>
                <html lang="he" dir="rtl">
                  <head>
                    <meta charset="UTF-8">
                    <title>גישה מוגנת</title>
                    <style>
                      body { font-family: Arial, sans-serif; background: #0f172a; color: #f8fafc; display: flex; align-items: center; justify-content: center; min-height: 100vh; }
                      .card { background: #1e293b; padding: 32px; border-radius: 16px; width: 100%; max-width: 360px; box-shadow: 0 10px 30px rgba(0,0,0,0.4); }
                      input { width: 100%; padding: 12px; border-radius: 10px; border: 1px solid #334155; background: #0f172a; color: #fff; }
                      button { width: 100%; margin-top: 12px; padding: 12px; border-radius: 10px; border: none; background: #f59e0b; color: #0f172a; font-weight: bold; cursor: pointer; }
                    </style>
                  </head>
                  <body>
                    <form class="card" method="get" action="/dafdaf">
                      <h2>גישה לגיבוי</h2>
                      <p>הזן סיסמה כדי להמשיך.</p>
                      <input type="password" name="password" placeholder="סיסמה" required>
                      <button type="submit">כניסה</button>
                    </form>
                  </body>
                </html>
                """
            )
            return
        password = self._get_query_value(params, "password", "")
        self._send_html(
            f"""
            <!DOCTYPE html>
            <html lang="he" dir="rtl">
              <head>
                <meta charset="UTF-8">
                <title>גיבוי נתונים</title>
                <style>
                  body {{ font-family: Arial, sans-serif; background: #0f172a; color: #f8fafc; padding: 32px; }}
                  .card {{ background: #1e293b; padding: 24px; border-radius: 16px; max-width: 720px; margin: 0 auto 24px; }}
                  textarea {{ width: 100%; min-height: 220px; background: #0f172a; color: #fff; border: 1px solid #334155; border-radius: 10px; padding: 12px; }}
                  button, a {{ display: inline-block; padding: 10px 16px; border-radius: 10px; background: #38bdf8; color: #0f172a; font-weight: bold; text-decoration: none; border: none; cursor: pointer; }}
                  .muted {{ color: #94a3b8; font-size: 14px; }}
                  .status {{ margin-top: 12px; font-size: 14px; }}
                </style>
              </head>
              <body>
                <div class="card">
                  <h2>יצוא נתונים</h2>
                  <p class="muted">הורד קובץ גיבוי JSON לכל השיבוצים.</p>
                  <a href="/dafdaf/export?password={password}">הורד גיבוי</a>
                </div>
                <div class="card">
                  <h2>יבוא נתונים</h2>
                  <p class="muted">הדבק כאן קובץ JSON מהגיבוי ולחץ על "יבא".</p>
                  <textarea id="import-data" placeholder='[{{"masechet":"ברכות","daf":2,"name":"...","dedication":"","learned":false,"is_full_masechet":false}}]'></textarea>
                  <button type="button" onclick="importData()">יבא נתונים</button>
                  <div id="import-status" class="status"></div>
                </div>
                <script>
                  const password = new URLSearchParams(window.location.search).get('password') || '';
                  async function importData() {{
                    const statusEl = document.getElementById('import-status');
                    statusEl.textContent = '';
                    const raw = document.getElementById('import-data').value.trim();
                    if (!raw) {{
                      statusEl.textContent = 'אנא הדבק נתונים.';
                      return;
                    }}
                    let payload;
                    try {{
                      payload = JSON.parse(raw);
                    }} catch (error) {{
                      statusEl.textContent = 'JSON לא תקין.';
                      return;
                    }}
                    const response = await fetch(`/dafdaf/import?password=${{encodeURIComponent(password)}}`, {{
                      method: 'POST',
                      headers: {{ 'Content-Type': 'application/json' }},
                      body: JSON.stringify(payload)
                    }});
                    if (response.ok) {{
                      const data = await response.json();
                      statusEl.textContent = `עודכנו ${{data.count ?? 0}} שיבוצים.`;
                      return;
                    }}
                    statusEl.textContent = 'שגיאה ביבוא הנתונים.';
                  }}
                </script>
              </body>
            </html>
            """
        )

    def _serve_backup_export(self, parsed) -> None:
        if not self._is_backup_authorized(parsed):
            self.send_error(403, "Forbidden")
            return
        payload = store.list_assignments()
        body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header(
            "Content-Disposition",
            'attachment; filename="daf-assignments.json"',
        )
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _handle_backup_import(self) -> None:
        parsed = urlparse(self.path)
        if not self._is_backup_authorized(parsed):
            self.send_error(403, "Forbidden")
            return
        try:
            payload = self._read_json_any()
        except PayloadTooLargeError:
            self._send_json({"error": "Payload too large"}, status=413)
            return
        if payload is None:
            self._send_json({"error": "Invalid JSON"}, status=400)
            return
        if isinstance(payload, dict) and "assignments" in payload:
            assignments_payload = payload.get("assignments")
        else:
            assignments_payload = payload
        if not isinstance(assignments_payload, list):
            self._send_json({"error": "Invalid assignments payload"}, status=400)
            return
        try:
            count = store.replace_assignments(assignments_payload)
        except ValueError:
            self._send_json({"error": "Missing required fields"}, status=400)
            return
        except sqlite3.IntegrityError:
            self._send_json({"error": "Assignment already exists"}, status=409)
            return
        self._send_json({"ok": True, "count": count})

    def _send_html(self, content: str, *, status: int = 200) -> None:
        body = content.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _serve_ics(self, parsed) -> None:
        params = parse_qs(parsed.query)
        title = self._get_query_value(params, "title", "לימוד דפים")
        details = self._get_query_value(params, "details", "")
        start_date = self._parse_date_param(params, "start", date.today())
        end_date = self._parse_date_param(params, "end", start_date + timedelta(days=1))
        ics_payload = self._build_ics(title, details, start_date, end_date)
        body = ics_payload.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/calendar; charset=utf-8")
        self.send_header(
            "Content-Disposition",
            'attachment; filename="daf-assignment.ics"',
        )
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self) -> dict | None:
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            return None
        if length > MAX_CONTENT_LENGTH:
            raise PayloadTooLargeError
        if length <= 0:
            return None
        try:
            payload = json.loads(self.rfile.read(length))
        except json.JSONDecodeError:
            return None
        if not isinstance(payload, dict):
            return None
        return payload

    def _read_json_any(self) -> dict | list | None:
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            return None
        if length > MAX_CONTENT_LENGTH:
            raise PayloadTooLargeError
        if length <= 0:
            return None
        try:
            payload = json.loads(self.rfile.read(length))
        except json.JSONDecodeError:
            return None
        if not isinstance(payload, (dict, list)):
            return None
        return payload

    @staticmethod
    def _escape_ics_text(value: str) -> str:
        return (
            str(value)
            .replace("\\", "\\\\")
            .replace(";", "\\;")
            .replace(",", "\\,")
            .replace("\n", "\\n")
        )

    def _build_ics(self, title: str, details: str, start_date: date, end_date: date) -> str:
        uid = f"{uuid.uuid4()}@daf"
        dtstamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        start_value = start_date.strftime("%Y%m%d")
        end_value = end_date.strftime("%Y%m%d")
        return "\r\n".join(
            [
                "BEGIN:VCALENDAR",
                "VERSION:2.0",
                "PRODID:-//Daf//Assignments//HE",
                "CALSCALE:GREGORIAN",
                "BEGIN:VEVENT",
                f"UID:{uid}",
                f"DTSTAMP:{dtstamp}",
                f"DTSTART;VALUE=DATE:{start_value}",
                f"DTEND;VALUE=DATE:{end_value}",
                f"SUMMARY:{self._escape_ics_text(title)}",
                f"DESCRIPTION:{self._escape_ics_text(details)}",
                "END:VEVENT",
                "END:VCALENDAR",
                "",
            ]
        )

    @staticmethod
    def _get_query_value(params: dict, key: str, default: str) -> str:
        value = params.get(key, [""])
        if not value:
            return default
        return value[0] or default

    @staticmethod
    def _is_backup_authorized(parsed) -> bool:
        params = parse_qs(parsed.query)
        return RequestHandler._get_query_value(params, "password", "") == BACKUP_PASSWORD

    @staticmethod
    def _parse_date_param(params: dict, key: str, default_value: date) -> date:
        value = params.get(key, [""])
        if value and value[0]:
            try:
                return datetime.strptime(value[0], "%Y%m%d").date()
            except ValueError:
                return default_value
        return default_value

    def _extract_id(self) -> int | None:
        try:
            return int(self.path.rsplit("/", 1)[-1])
        except (ValueError, IndexError):
            return None

    def _send_json(self, payload: dict | list, *, status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def run() -> None:
    port = int(os.environ.get("PORT", "8000"))
    server = HTTPServer(("", port), RequestHandler)
    print(f"Serving on http://0.0.0.0:{port}")
    server.serve_forever()


if __name__ == "__main__":
    run()
