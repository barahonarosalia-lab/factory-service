import os
import json
import uuid
from datetime import datetime
from typing import Any, Dict

import requests
from flask import Flask, request, jsonify

app = Flask(__name__)

BASE_OUTPUT_DIR = os.getenv("OUTPUT_BASE_DIR", "/data/outputs")

FOLDERS = ["inputs", "pod", "digital", "palette", "pbn", "procreate", "previews"]


def utc_now() -> str:
    return datetime.utcnow().isoformat() + "Z"


def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def write_json(path: str, data: Dict[str, Any]):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def append_log(log_path: str, msg: str):
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"[{utc_now()}] {msg}\n")


def download_file(url: str, out_path: str, timeout: int = 60):
    r = requests.get(url, stream=True, timeout=timeout)
    r.raise_for_status()
    with open(out_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=1024 * 256):
            if chunk:
                f.write(chunk)


def validate_payload(p: Dict[str, Any]) -> str | None:
    if not isinstance(p, dict):
        return "Payload must be a JSON object."

    if not p.get("job_id"):
        return "Missing job_id."
    if not p.get("assets") or not isinstance(p["assets"], list):
        return "Missing assets array."

    # Expect first asset to have mj.image_url
    a0 = p["assets"][0]
    mj = a0.get("mj", {})
    if not isinstance(mj, dict) or not mj.get("image_url"):
        return "assets[0].mj.image_url is required (selected_image_url)."

    return None


@app.get("/health")
def health():
    return jsonify({"ok": True, "service": "factory-service", "time": utc_now()})


@app.post("/run")
def run():
    payload = request.get_json(force=True, silent=True)
    err = validate_payload(payload)
    if err:
        return jsonify({"ok": False, "error": err}), 400

    job_id = payload["job_id"]
    output_folder = os.path.join(BASE_OUTPUT_DIR, job_id)
    ensure_dir(output_folder)

    # Create folder tree
    for f in FOLDERS:
        ensure_dir(os.path.join(output_folder, f))

    log_path = os.path.join(output_folder, "logs.txt")
    append_log(log_path, f"RUN start job_id={job_id}")

    # Download source image
    src_url = payload["assets"][0]["mj"]["image_url"]
    ext = ".png"  # safe default; Discord often returns PNGs
    source_path = os.path.join(output_folder, "inputs", f"source{ext}")

    append_log(log_path, f"Downloading source image from: {src_url}")
    try:
        download_file(src_url, source_path)
    except Exception as e:
        append_log(log_path, f"ERROR downloading image: {repr(e)}")
        # write a failure manifest
        manifest_path = os.path.join(output_folder, "manifest.json")
        manifest = {
            "job_id": job_id,
            "status": "error",
            "error": f"download_failed: {repr(e)}",
            "created_utc": utc_now(),
            "output_folder": output_folder,
            "source_url": src_url,
        }
        write_json(manifest_path, manifest)
        return jsonify({"ok": False, "error": "download_failed", "manifest_path": manifest_path}), 500

    append_log(log_path, f"Saved source image: {source_path}")

    # Write manifest
    manifest_path = os.path.join(output_folder, "manifest.json")
    manifest = {
        "job_id": job_id,
        "collection_id": payload.get("collection_id"),
        "trend": payload.get("trend", {}),
        "difficulty": payload.get("difficulty"),
        "assets": payload.get("assets", []),
        "requested_outputs": payload.get("requested_outputs", {}),
        "status": "phase0_complete",
        "created_utc": utc_now(),
        "output_folder": output_folder,
        "paths": {
            "manifest": manifest_path,
            "logs": log_path,
            "source_image": source_path,
        },
        "files": [
            {"path": source_path, "type": "input", "role": "source_image"}
        ],
    }
    write_json(manifest_path, manifest)
    append_log(log_path, f"Wrote manifest: {manifest_path}")
    append_log(log_path, "RUN complete")

    return jsonify({
        "ok": True,
        "job_id": job_id,
        "output_folder": output_folder,
        "manifest_path": manifest_path
    })
