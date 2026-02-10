#!/usr/bin/env python3
"""
Phase 0 Worker: validates a single job payload and creates:
  /outputs/{job_id}/
    manifest.json
    logs.txt
    pod/
    digital/
    palette/
    pbn/
    procreate/
    previews/
    inputs/

Usage:
  python worker_phase0.py --payload payload.json
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime
from typing import Any, Dict, List


REQUIRED_TOP_KEYS = ["job_id", "collection_id", "trend", "difficulty", "providers", "requested_outputs", "assets", "io"]


def now_utc() -> str:
    return datetime.utcnow().isoformat() + "Z"


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def write_text(path: str, text: str) -> None:
    with open(path, "a", encoding="utf-8") as f:
        f.write(text + "\n")


def validate_payload(p: Dict[str, Any]) -> List[str]:
    errors: List[str] = []

    for k in REQUIRED_TOP_KEYS:
        if k not in p:
            errors.append(f"Missing top-level key: {k}")

    if "job_id" in p and not isinstance(p["job_id"], str):
        errors.append("job_id must be a string")

    if "assets" in p and not isinstance(p["assets"], list):
        errors.append("assets must be an array")

    # Validate assets structure
    assets = p.get("assets", [])
    seen_ids = set()
    for i, a in enumerate(assets):
        if not isinstance(a, dict):
            errors.append(f"assets[{i}] must be an object")
            continue
        aid = a.get("asset_id")
        if not aid:
            errors.append(f"assets[{i}] missing asset_id")
        elif aid in seen_ids:
            errors.append(f"Duplicate asset_id: {aid}")
        else:
            seen_ids.add(aid)

        lane = a.get("lane")
        if lane not in ("pod_raster", "pbn", "generated", "pod_raster_pbn", None):
            # allow None for older transforms but encourage setting lane
            pass

        # MJ fields required if it needs an image
        if a.get("generator") == "bathmat_texture_from_palette":
            # should not include mj fields
            continue

        mj = a.get("mj")
        if mj:
            if not isinstance(mj, dict):
                errors.append(f"assets[{i}].mj must be object")
            else:
                if not mj.get("visual_prompt") and not mj.get("image_url"):
                    errors.append(f"assets[{i}] needs mj.visual_prompt or mj.image_url")

        pbn = a.get("pbn")
        if pbn and not isinstance(pbn, dict):
            errors.append(f"assets[{i}].pbn must be object")

    # Providers sanity
    providers = p.get("providers", {})
    if not isinstance(providers, dict):
        errors.append("providers must be object")
    else:
        if "prodigi" not in providers or "printful" not in providers:
            errors.append("providers must include prodigi and printful")

    return errors


def build_manifest(payload: Dict[str, Any], root: str) -> Dict[str, Any]:
    return {
        "job_id": payload["job_id"],
        "collection_id": payload.get("collection_id"),
        "created_utc": now_utc(),
        "status": "phase0_initialized",
        "trend": payload.get("trend", {}),
        "difficulty": payload.get("difficulty"),
        "providers": payload.get("providers", {}),
        "requested_outputs": payload.get("requested_outputs", {}),
        "paths": {
            "root": root,
            "inputs": os.path.join(root, "inputs"),
            "pod": os.path.join(root, "pod"),
            "digital": os.path.join(root, "digital"),
            "palette": os.path.join(root, "palette"),
            "pbn": os.path.join(root, "pbn"),
            "procreate": os.path.join(root, "procreate"),
            "previews": os.path.join(root, "previews"),
            "logs": os.path.join(root, "logs.txt"),
            "manifest": os.path.join(root, "manifest.json")
        },
        "files": [],
        "assets": payload.get("assets", [])
    }


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--payload", required=True, help="Path to a single job payload JSON file")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    with open(args.payload, "r", encoding="utf-8") as f:
        payload = json.load(f)

    # Validate
    errors = validate_payload(payload)

    out_base = payload.get("io", {}).get("output_base_dir") or "/data/outputs"
    job_id = payload.get("job_id", "job_unknown")
    root = os.path.join(out_base, job_id)

    ensure_dir(root)
    log_path = os.path.join(root, "logs.txt")

    write_text(log_path, f"[{now_utc()}] Phase0 start")
    write_text(log_path, f"[{now_utc()}] job_id={job_id}")
    write_text(log_path, f"[{now_utc()}] output_base_dir={out_base}")

    if errors:
        write_text(log_path, f"[{now_utc()}] VALIDATION FAILED:")
        for e in errors:
            write_text(log_path, f"  - {e}")
        # Still write a manifest with failure status for debugging
        manifest = build_manifest(payload, root)
        manifest["status"] = "failed_validation"
        manifest["validation_errors"] = errors
        with open(os.path.join(root, "manifest.json"), "w", encoding="utf-8") as mf:
            json.dump(manifest, mf, indent=2)
        print(json.dumps({"ok": False, "errors": errors, "manifest": manifest["paths"]["manifest"]}, indent=2))
        return

    # Create folder tree
    for d in ["inputs", "pod", "digital", "palette", "pbn", "procreate", "previews"]:
        ensure_dir(os.path.join(root, d))

    manifest = build_manifest(payload, root)
    with open(os.path.join(root, "manifest.json"), "w", encoding="utf-8") as mf:
        json.dump(manifest, mf, indent=2)

    write_text(log_path, f"[{now_utc()}] Phase0 complete: folder tree + manifest written")
    print(json.dumps({"ok": True, "root": root, "manifest": manifest["paths"]["manifest"]}, indent=2))


if __name__ == "__main__":
    main()
