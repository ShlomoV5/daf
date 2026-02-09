import base64
import io
import json
import os
import shutil
import sqlite3
import sys
import tempfile
import threading
import time
import urllib.request
import uuid
import zipfile
from datetime import date, datetime, timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse


BASE_DIR = Path(__file__).resolve().parent
HTML_PATH = BASE_DIR / "base.html"
DB_PATH = os.environ.get("DB_PATH", str(BASE_DIR / "assignments.db"))
MAX_CONTENT_LENGTH = 1_000_000
BACKUP_PASSWORD = os.environ.get("BACKUP_PASSWORD", "123123")
UPDATE_REPO = os.environ.get("GITHUB_REPO", "ShlomoV5/daf")
UPDATE_BRANCH = os.environ.get("GITHUB_BRANCH", "main")
UPDATE_REPO_ZIP_URL = os.environ.get("GITHUB_REPO_ZIP_URL")
UPDATE_DOWNLOAD_TIMEOUT = int(os.environ.get("GITHUB_DOWNLOAD_TIMEOUT", "20"))
UPDATE_RESTART_DELAY = float(os.environ.get("GITHUB_RESTART_DELAY", "1"))


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
                    daf_end INTEGER,
                    name TEXT NOT NULL,
                    dedication TEXT,
                    learned INTEGER NOT NULL DEFAULT 0,
                    is_full_masechet INTEGER NOT NULL DEFAULT 0,
                    UNIQUE (masechet, daf)
                )
                """
            )
            columns = {
                row["name"]
                for row in connection.execute("PRAGMA table_info(assignments)").fetchall()
            }
            if "daf_end" not in columns:
                connection.execute("ALTER TABLE assignments ADD COLUMN daf_end INTEGER")
            connection.execute("UPDATE assignments SET daf_end = daf WHERE daf_end IS NULL")

    def list_assignments(self) -> list[dict]:
        with self._get_connection() as connection:
            cursor = connection.execute(
                "SELECT id, masechet, daf, daf_end, name, dedication, learned, is_full_masechet FROM assignments "
                "ORDER BY masechet, daf"
            )
            rows = cursor.fetchall()
        return [self._row_to_dict(row) for row in rows]

    def get_assignment(self, assignment_id: int) -> dict | None:
        with self._get_connection() as connection:
            cursor = connection.execute(
                "SELECT id, masechet, daf, daf_end, name, dedication, learned, is_full_masechet "
                "FROM assignments WHERE id = ?",
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
        self._validate_payload_ranges(records)
        assignment_ids = []
        with self._get_connection() as connection:
            for record in records:
                if self._has_overlap(
                    connection, record["masechet"], record["daf"], record["daf_end"]
                ):
                    raise sqlite3.IntegrityError("Assignment overlap")
                cursor = connection.execute(
                    """
                    INSERT INTO assignments (masechet, daf, daf_end, name, dedication, learned, is_full_masechet)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        record["masechet"],
                        record["daf"],
                        record["daf_end"],
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
            if self._has_overlap(
                connection,
                record["masechet"],
                record["daf"],
                record["daf_end"],
                exclude_id=assignment_id,
            ):
                raise sqlite3.IntegrityError("Assignment overlap")
            connection.execute(
                """
                UPDATE assignments
                SET masechet = ?, daf = ?, daf_end = ?, name = ?, dedication = ?, learned = ?, is_full_masechet = ?
                WHERE id = ?
                """,
                (
                    record["masechet"],
                    record["daf"],
                    record["daf_end"],
                    record["name"],
                    record.get("dedication"),
                    int(record.get("learned", False)),
                    int(record.get("is_full_masechet", False)),
                    assignment_id,
                ),
            )
        return self.get_assignment(assignment_id)

    def update_assignment_daf(
        self, assignment_id: int, target_daf: int, learned: bool
    ) -> dict | None:
        record = self.get_assignment(assignment_id)
        if not record:
            return None
        start = record["daf"]
        end = record.get("daf_end", start)
        if target_daf < start or target_daf > end:
            return None
        if start == end:
            return self.update_assignment(assignment_id, {"learned": learned})
        if record["learned"] == learned:
            return record
        segments = []
        if start < target_daf:
            segments.append((start, target_daf - 1, record["learned"]))
        segments.append((target_daf, target_daf, learned))
        if target_daf < end:
            segments.append((target_daf + 1, end, record["learned"]))
        with self._get_connection() as connection:
            connection.execute("DELETE FROM assignments WHERE id = ?", (assignment_id,))
            for segment_start, segment_end, segment_learned in segments:
                connection.execute(
                    """
                    INSERT INTO assignments (masechet, daf, daf_end, name, dedication, learned, is_full_masechet)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        record["masechet"],
                        segment_start,
                        segment_end,
                        record["name"],
                        record.get("dedication"),
                        int(segment_learned),
                        int(record.get("is_full_masechet", False)),
                    ),
                )
        return self._get_assignment_covering(record["masechet"], target_daf)

    def delete_assignment(self, assignment_id: int) -> bool:
        with self._get_connection() as connection:
            cursor = connection.execute(
                "DELETE FROM assignments WHERE id = ?",
                (assignment_id,),
            )
        return cursor.rowcount > 0

    def delete_assignment_daf(self, assignment_id: int, target_daf: int) -> bool:
        record = self.get_assignment(assignment_id)
        if not record:
            return False
        start = record["daf"]
        end = record.get("daf_end", start)
        if target_daf < start or target_daf > end:
            return False
        with self._get_connection() as connection:
            if start == end:
                cursor = connection.execute(
                    "DELETE FROM assignments WHERE id = ?",
                    (assignment_id,),
                )
                return cursor.rowcount > 0
            if target_daf == start:
                connection.execute(
                    "UPDATE assignments SET daf = ?, daf_end = ? WHERE id = ?",
                    (start + 1, end, assignment_id),
                )
                return True
            if target_daf == end:
                connection.execute(
                    "UPDATE assignments SET daf_end = ? WHERE id = ?",
                    (end - 1, assignment_id),
                )
                return True
            connection.execute("DELETE FROM assignments WHERE id = ?", (assignment_id,))
            for segment_start, segment_end in (
                (start, target_daf - 1),
                (target_daf + 1, end),
            ):
                connection.execute(
                    """
                    INSERT INTO assignments (masechet, daf, daf_end, name, dedication, learned, is_full_masechet)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        record["masechet"],
                        segment_start,
                        segment_end,
                        record["name"],
                        record.get("dedication"),
                        int(record.get("learned", False)),
                        int(record.get("is_full_masechet", False)),
                    ),
                )
        return True

    def replace_assignments(self, payloads: list[dict]) -> int:
        if payloads is None or not isinstance(payloads, list):
            raise ValueError("Missing required fields")
        records = [self._parse_payload(payload, require_fields=True) for payload in payloads]
        self._validate_payload_ranges(records)
        with self._get_connection() as connection:
            connection.execute("DELETE FROM assignments")
            for record in records:
                connection.execute(
                    """
                    INSERT INTO assignments (masechet, daf, daf_end, name, dedication, learned, is_full_masechet)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        record["masechet"],
                        record["daf"],
                        record["daf_end"],
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
            "daf_end": row["daf_end"] if row["daf_end"] is not None else row["daf"],
            "name": row["name"],
            "dedication": row["dedication"] or "",
            "learned": bool(row["learned"]),
            "is_full_masechet": bool(row["is_full_masechet"]),
        }

    @staticmethod
    def _validate_payload_ranges(records: list[dict]) -> None:
        grouped: dict[str, list[tuple[int, int]]] = {}
        for record in records:
            grouped.setdefault(record["masechet"], []).append(
                (record["daf"], record["daf_end"])
            )
        for ranges in grouped.values():
            ranges.sort(key=lambda item: item[0])
            previous_end = None
            for start, end in ranges:
                if previous_end is not None and start <= previous_end:
                    raise sqlite3.IntegrityError("Overlapping assignments")
                previous_end = end

    @staticmethod
    def _has_overlap(
        connection: sqlite3.Connection,
        masechet: str,
        start: int,
        end: int,
        *,
        exclude_id: int | None = None,
    ) -> bool:
        query = (
            "SELECT 1 FROM assignments "
            "WHERE masechet = ? "
            "AND NOT (COALESCE(daf_end, daf) < ? OR daf > ?)"
        )
        params: list[int | str] = [masechet, start, end]
        if exclude_id is not None:
            query += " AND id != ?"
            params.append(exclude_id)
        cursor = connection.execute(query, params)
        return cursor.fetchone() is not None

    def _get_assignment_covering(self, masechet: str, daf: int) -> dict | None:
        with self._get_connection() as connection:
            cursor = connection.execute(
                """
                SELECT id, masechet, daf, daf_end, name, dedication, learned, is_full_masechet
                FROM assignments
                WHERE masechet = ? AND ? BETWEEN daf AND COALESCE(daf_end, daf)
                LIMIT 1
                """,
                (masechet, daf),
            )
            row = cursor.fetchone()
        return self._row_to_dict(row) if row else None

    def _get_assignments_by_ids(self, assignment_ids: list[int]) -> list[dict]:
        if not assignment_ids:
            return []
        placeholders = ",".join(["?"] * len(assignment_ids))
        with self._get_connection() as connection:
            query = (
                "SELECT id, masechet, daf, daf_end, name, dedication, learned, is_full_masechet "
                "FROM assignments WHERE id IN ({}) ORDER BY id"
            ).format(placeholders)
            cursor = connection.execute(query, assignment_ids)
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
        daf_end = payload.get("daf_end") if payload else None
        if daf_end is None:
            daf_end = defaults.get("daf_end")

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
        if daf_end is None:
            daf_end_value = daf_value
        else:
            try:
                daf_end_value = int(daf_end)
            except (TypeError, ValueError) as error:
                raise ValueError("Invalid daf_end") from error
        if daf_end_value < daf_value:
            raise ValueError("Invalid daf_end")

        return {
            "masechet": masechet,
            "name": name,
            "daf": daf_value,
            "daf_end": daf_end_value,
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
            self._serve_backup_page()
            return
        if path == "/dafdaf/export":
            self._serve_backup_export()
            return
        if path == "/יחד אחים.png":
            self._serve_file(BASE_DIR / "יחד אחים.png", "image/png")
            return
        self.send_error(404, "Not Found")

    def do_POST(self) -> None:
        if self.path.startswith("/dafdaf/import"):
            self._handle_backup_import()
            return
        if self.path == "/dafdaf/update":
            self._handle_code_update()
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
            target_daf = None
            if isinstance(payload, dict):
                target_daf = payload.get("target_daf", payload.get("targetDaf"))
            if target_daf is not None:
                target_value = int(target_daf)
                if "learned" not in payload:
                    self._send_json({"error": "Invalid payload"}, status=400)
                    return
                record = store.update_assignment_daf(
                    assignment_id, target_value, bool(payload.get("learned"))
                )
            else:
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
        parsed = urlparse(self.path)
        if not parsed.path.startswith("/api/assignments/"):
            self.send_error(404, "Not Found")
            return
        assignment_id = self._extract_id(parsed.path)
        if assignment_id is None:
            self._send_json({"error": "Invalid assignment id"}, status=400)
            return
        params = parse_qs(parsed.query)
        target_daf = self._parse_int_param(params, "daf")
        if "daf" in params and target_daf is None:
            self._send_json({"error": "Invalid daf"}, status=400)
            return
        if target_daf is not None:
            deleted = store.delete_assignment_daf(assignment_id, target_daf)
        else:
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

    def _serve_backup_page(self) -> None:
        if not self._require_backup_auth():
            return
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
                  <a href="/dafdaf/export">הורד גיבוי</a>
                </div>
                <div class="card">
                  <h2>עדכון מ-GitHub</h2>
                  <p class="muted">משיכת הקוד מתוך {UPDATE_REPO} (ענף {UPDATE_BRANCH}). הנתונים נשמרים.</p>
                  <button type="button" onclick="updateCodebase()">עדכן קוד</button>
                  <div id="update-status" class="status"></div>
                </div>
                <div class="card">
                  <h2>יבוא נתונים</h2>
                  <p class="muted">הדבק כאן קובץ JSON מהגיבוי ולחץ על "ייבא".</p>
                  <textarea id="import-data" placeholder='[{{"masechet":"ברכות","daf":2,"name":"...","dedication":"","learned":false,"is_full_masechet":false}}]'></textarea>
                  <button type="button" onclick="importData()">ייבא נתונים</button>
                  <div id="import-status" class="status"></div>
                </div>
                <script>
                  const UPDATE_RELOAD_DELAY_MS = 2500;
                  async function updateCodebase() {{
                    const statusEl = document.getElementById('update-status');
                    statusEl.textContent = 'מעדכן קוד...';
                    const response = await fetch('/dafdaf/update', {{ method: 'POST' }});
                    if (response.ok) {{
                      const data = await response.json();
                      statusEl.textContent = data.message || 'עודכן בהצלחה. אם הסביבה מאפשרת, השרת ייטען מחדש.';
                      setTimeout(() => window.location.reload(), UPDATE_RELOAD_DELAY_MS);
                      return;
                    }}
                    statusEl.textContent = 'שגיאה בעדכון הקוד.';
                  }}

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
                    const response = await fetch('/dafdaf/import', {{
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

    def _serve_backup_export(self) -> None:
        if not self._require_backup_auth():
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
        if not self._require_backup_auth():
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

    def _handle_code_update(self) -> None:
        if not self._require_backup_auth():
            return
        try:
            updated_count = self._perform_code_update()
        except (OSError, ValueError, zipfile.BadZipFile) as error:
            error_name = type(error).__name__
            self._send_json(
                {"error": "Update failed", "details": f"{error_name}: {error}"},
                status=500,
            )
            return
        self._send_json(
            {
                "ok": True,
                "message": (
                    f"עודכנו {updated_count} קבצים. אם הסביבה מאפשרת, השרת ייטען מחדש."
                ),
            }
        )
        self._schedule_restart()

    def _perform_code_update(self) -> int:
        update_url = self._get_update_url()
        with urllib.request.urlopen(update_url, timeout=UPDATE_DOWNLOAD_TIMEOUT) as response:
            status = getattr(response, "status", 200)
            if status not in (200, None):
                raise ValueError(
                    f"Failed to download update from {update_url} (status {status})"
                )
            payload = response.read()
        with tempfile.TemporaryDirectory() as temp_dir:
            extract_dir = Path(temp_dir) / "repo"
            extract_dir.mkdir(parents=True, exist_ok=True)
            with zipfile.ZipFile(io.BytesIO(payload)) as archive:
                self._safe_extract_archive(archive, extract_dir)
            repo_root = self._resolve_repo_root(extract_dir)
            return self._sync_repo_files(repo_root)

    @staticmethod
    def _get_update_url() -> str:
        if UPDATE_REPO_ZIP_URL:
            url = UPDATE_REPO_ZIP_URL
        else:
            url = (
                f"https://github.com/{UPDATE_REPO}/archive/refs/heads/{UPDATE_BRANCH}.zip"
            )
        RequestHandler._validate_update_url(url)
        return url

    @staticmethod
    def _validate_update_url(url: str) -> None:
        parsed = urlparse(url)
        if parsed.scheme == "file":
            return
        if parsed.scheme != "https":
            raise ValueError("Invalid update URL scheme")
        host = parsed.hostname or ""
        allowed_hosts = {
            "github.com",
            "codeload.github.com",
            "objects.githubusercontent.com",
        }
        if host not in allowed_hosts:
            raise ValueError("Invalid update URL host")

    @staticmethod
    def _safe_extract_archive(archive: zipfile.ZipFile, destination: Path) -> None:
        for member in archive.infolist():
            member_path = Path(member.filename)
            if member_path.is_absolute() or ".." in member_path.parts:
                continue
            target = destination / member_path
            if member.is_dir():
                target.mkdir(parents=True, exist_ok=True)
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            with archive.open(member) as source, open(target, "wb") as target_file:
                shutil.copyfileobj(source, target_file)

    @staticmethod
    def _resolve_repo_root(extract_dir: Path) -> Path:
        candidates = [
            entry
            for entry in extract_dir.iterdir()
            if entry.is_dir() and entry.name != ".git"
        ]
        if len(candidates) == 1:
            return candidates[0]
        return extract_dir

    @staticmethod
    def _sync_repo_files(repo_root: Path) -> int:
        skip_dirs = {".git", "__pycache__"}
        skip_files = {Path(DB_PATH).name}
        updated_count = 0
        for path in repo_root.rglob("*"):
            relative_path = path.relative_to(repo_root)
            if any(part in skip_dirs for part in relative_path.parts):
                continue
            if relative_path.name in skip_files:
                continue
            destination = BASE_DIR / relative_path
            if path.is_dir():
                destination.mkdir(parents=True, exist_ok=True)
                continue
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, destination)
            updated_count += 1
        return updated_count

    @staticmethod
    def _schedule_restart() -> None:
        def _restart() -> None:
            time.sleep(UPDATE_RESTART_DELAY)
            try:
                os.execv(sys.executable, [sys.executable, *sys.argv])
            except OSError as error:
                print(f"Failed to restart server: {error}")

        threading.Thread(target=_restart, daemon=True).start()

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

    def _is_backup_authorized(self) -> bool:
        auth_header = self.headers.get("Authorization", "")
        if not auth_header.startswith("Basic "):
            return False
        encoded = auth_header.split(" ", 1)[1]
        try:
            decoded = base64.b64decode(encoded).decode("utf-8")
        except (ValueError, UnicodeDecodeError):
            return False
        _, _, password = decoded.partition(":")
        return password == BACKUP_PASSWORD

    def _require_backup_auth(self) -> bool:
        if self._is_backup_authorized():
            return True
        self.send_response(401)
        self.send_header("WWW-Authenticate", 'Basic realm="Daf Backup"')
        self.end_headers()
        return False

    @staticmethod
    def _parse_date_param(params: dict, key: str, default_value: date) -> date:
        value = params.get(key, [""])
        if value and value[0]:
            try:
                return datetime.strptime(value[0], "%Y%m%d").date()
            except ValueError:
                return default_value
        return default_value

    def _extract_id(self, path: str | None = None) -> int | None:
        raw_path = path or self.path
        try:
            return int(raw_path.rsplit("/", 1)[-1])
        except (ValueError, IndexError):
            return None

    @staticmethod
    def _parse_int_param(params: dict, key: str) -> int | None:
        value = params.get(key, [""])
        if not value or not value[0]:
            return None
        try:
            return int(value[0])
        except ValueError:
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
