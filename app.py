import json
import os
import sqlite3
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import urlparse


BASE_DIR = Path(__file__).resolve().parent
HTML_PATH = BASE_DIR / "base.html"
DB_PATH = os.environ.get("DB_PATH", str(BASE_DIR / "assignments.db"))


class AssignmentStore:
    def __init__(self, db_path: str) -> None:
        self.connection = sqlite3.connect(db_path, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        self.connection.execute(
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
        self.connection.commit()

    def list_assignments(self) -> list[dict]:
        cursor = self.connection.execute(
            "SELECT id, masechet, daf, name, dedication, learned, is_full_masechet FROM assignments "
            "ORDER BY masechet, daf"
        )
        return [self._row_to_dict(row) for row in cursor.fetchall()]

    def get_assignment(self, assignment_id: int) -> dict | None:
        cursor = self.connection.execute(
            "SELECT id, masechet, daf, name, dedication, learned, is_full_masechet FROM assignments WHERE id = ?",
            (assignment_id,),
        )
        row = cursor.fetchone()
        return self._row_to_dict(row) if row else None

    def create_assignment(self, payload: dict) -> dict:
        record = self._parse_payload(payload, require_fields=True)
        cursor = self.connection.execute(
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
        self.connection.commit()
        return self.get_assignment(cursor.lastrowid)

    def update_assignment(self, assignment_id: int, payload: dict) -> dict | None:
        existing = self.get_assignment(assignment_id)
        if not existing:
            return None
        record = self._parse_payload(payload, require_fields=False, defaults=existing)
        self.connection.execute(
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
        self.connection.commit()
        return self.get_assignment(assignment_id)

    def delete_assignment(self, assignment_id: int) -> bool:
        cursor = self.connection.execute(
            "DELETE FROM assignments WHERE id = ?",
            (assignment_id,),
        )
        self.connection.commit()
        return cursor.rowcount > 0

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
        dedication = payload.get("dedication") if payload else defaults.get("dedication")
        learned = payload.get("learned") if payload else defaults.get("learned", False)
        is_full_masechet = payload.get("is_full_masechet") if payload else defaults.get("is_full_masechet", False)

        if require_fields and (not masechet or not name or daf is None):
            raise ValueError("Missing required fields")

        return {
            "masechet": masechet,
            "name": name,
            "daf": int(daf),
            "dedication": str(dedication).strip() if dedication is not None else "",
            "learned": bool(learned),
            "is_full_masechet": bool(is_full_masechet),
        }


store = AssignmentStore(DB_PATH)


class RequestHandler(BaseHTTPRequestHandler):
    server_version = "DafHTTP/1.0"

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path in ("/", "/base.html", "/index.html"):
            self._serve_html()
            return
        if parsed.path == "/api/assignments":
            self._send_json(store.list_assignments())
            return
        self.send_error(404, "Not Found")

    def do_POST(self) -> None:
        if self.path != "/api/assignments":
            self.send_error(404, "Not Found")
            return
        payload = self._read_json()
        if payload is None:
            self._send_json({"error": "Invalid JSON"}, status=400)
            return
        try:
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
        payload = self._read_json()
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

    def _read_json(self) -> dict | None:
        length = int(self.headers.get("Content-Length", "0"))
        if length <= 0:
            return None
        try:
            payload = json.loads(self.rfile.read(length))
        except json.JSONDecodeError:
            return None
        if not isinstance(payload, dict):
            return None
        return payload

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
