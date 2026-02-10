#!/usr/bin/env python3
"""
Transform your current trend-planner JSON (the big OpenAI response wrapper)
into normalized job payloads that a Render worker can execute.

Usage:
  python transform_trends_to_jobs.py --in raw.json --out jobs.json --collection-id LL_2026-02-10 --difficulty intermediate
"""

from __future__ import annotations

import argparse
import json
import re
from typing import Any, Dict, List, Optional


def slugify(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-+", "-", s).strip("-")
    return s[:80] if len(s) > 80 else s


def extract_inner_trends(raw: Any) -> List[Dict[str, Any]]:
    """
    Your provided file is an array with an OpenAI 'response' wrapper.
    The actual trends are inside output[0].content[0].text as a JSON string.
    We parse that out.

    If the input is already a list of trends, we return it as-is.
    """
    if isinstance(raw, list) and raw and isinstance(raw[0], dict) and "trend_name" in raw[0]:
        return raw  # already trends

    if not isinstance(raw, list) or not raw:
        raise ValueError("Unexpected input format: expected a list wrapper or list of trends.")

    wrapper = raw[0]
    output = wrapper.get("output", [])
    if not output:
        raise ValueError("No output field found in wrapper JSON.")

    # Find the text blob that contains ```json ... ```
    for msg in output:
        if msg.get("type") == "message":
            content = msg.get("content", [])
            for c in content:
                if c.get("type") == "output_text":
                    text = c.get("text", "")
                    # Pull JSON inside code fences if present
                    m = re.search(r"```json\s*(\[\s*{.*}\s*\])\s*```", text, flags=re.DOTALL)
                    if m:
                        inner = m.group(1)
                        return json.loads(inner)
                    # Or try parsing raw text as JSON
                    text = text.strip()
                    if text.startswith("[") and text.endswith("]"):
                        return json.loads(text)

    raise ValueError("Could not locate embedded trends JSON in the wrapper.")


def normalize_variations(trend: Dict[str, Any], difficulty: str) -> List[Dict[str, Any]]:
    style_variants = trend.get("style_variants", {})
    if difficulty not in style_variants:
        # fall back to intermediate if missing
        difficulty_key = "intermediate" if "intermediate" in style_variants else next(iter(style_variants.keys()))
    else:
        difficulty_key = difficulty

    diff_block = style_variants.get(difficulty_key, {})
    variations = diff_block.get("variations", [])

    trend_id = slugify(trend.get("trend_name", "trend"))
    assets: List[Dict[str, Any]] = []

    for v in variations:
        vtype = (v.get("type") or "asset").strip()
        ar = (v.get("aspect_ratio") or "").strip()

        # normalize AR text into a stable token
        ar_token = ar.replace("--ar", "").replace("--tile", "tile").replace(" ", "").replace(":", "x")
        if "tile" in ar:
            ar_token = "1x1_tile"

        asset_id = f"{trend_id}_{slugify(vtype)}_{slugify(ar_token or 'na')}"

        asset: Dict[str, Any] = {
            "asset_id": asset_id,
            "type": vtype,
            "purpose": v.get("purpose"),
            "aspect_ratio": ar_token or None,
            "lane": "pod_raster",
            "mj": {
                "visual_prompt": v.get("visual_prompt"),
                "image_url": None
            }
        }

        if v.get("pbn_twin_prompt"):
            asset["pbn"] = {
                "pbn_twin_prompt": v.get("pbn_twin_prompt"),
                "image_url": None
            }

        # Coordinate bath mat is NOT MJ-based in your rules
        if vtype.lower() == "coordinate" and "bath" in (v.get("purpose") or "").lower():
            asset["lane"] = "generated"
            asset.pop("mj", None)
            asset.pop("pbn", None)
            asset["generator"] = "bathmat_texture_from_palette"

        assets.append(asset)

    # Always ensure bath mat generator exists once per job
    if not any(a.get("generator") == "bathmat_texture_from_palette" for a in assets):
        assets.append({
            "asset_id": "bathmat_coordinate",
            "type": "Coordinate",
            "purpose": "Bath Mat Textures",
            "lane": "generated",
            "generator": "bathmat_texture_from_palette"
        })

    return assets


def build_job(trend: Dict[str, Any], collection_id: str, difficulty: str, output_base_dir: str) -> Dict[str, Any]:
    trend_name = trend.get("trend_name", "trend")
    trend_id = slugify(trend_name)
    brand = trend.get("assigned_brand", "brand")
    job_id = f"{collection_id}_{trend_id}_{difficulty}"

    job = {
        "job_id": job_id,
        "collection_id": collection_id,
        "trend": {
            "trend_id": trend_id,
            "trend_name": trend_name,
            "assigned_brand": brand,
            "category": trend.get("category"),
            "vibe": trend.get("vibe"),
            "reason": trend.get("reason"),
            "visual_motifs": trend.get("visual_motifs"),
            "color_palette_human": trend.get("color_palette")
        },
        "difficulty": difficulty,
        "providers": {
            "prodigi": ["tapestry", "woven_blanket"],
            "printful": ["pillow", "shower_curtain", "gift_wrap"]
        },
        "requested_outputs": {
            "digital_bundle": True,
            "palette": True,
            "bathmat_textures": True,
            "projector_mode": False,
            "pbn_lane": True
        },
        "assets": normalize_variations(trend, difficulty),
        "io": {
            "output_base_dir": output_base_dir
        }
    }
    return job


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--in", dest="in_path", required=True)
    p.add_argument("--out", dest="out_path", required=True)
    p.add_argument("--collection-id", required=True, help="e.g., LL_2026-02-10")
    p.add_argument("--difficulty", default="intermediate", choices=["kids", "beginner", "intermediate", "advanced"])
    p.add_argument("--output-base-dir", default="/data/outputs")
    p.add_argument("--limit", type=int, default=0, help="0 = all trends, else limit count")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    with open(args.in_path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    trends = extract_inner_trends(raw)
    if args.limit and args.limit > 0:
        trends = trends[: args.limit]

    jobs = [build_job(t, args.collection_id, args.difficulty, args.output_base_dir) for t in trends]

    with open(args.out_path, "w", encoding="utf-8") as f:
        json.dump(jobs, f, indent=2)

    print(f"Wrote {len(jobs)} job payload(s) to {args.out_path}")


if __name__ == "__main__":
    main()
