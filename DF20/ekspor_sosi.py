#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import os
import re
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import requests

NVDB_V4 = "https://nvdbapiles.atlas.vegvesen.no/vegobjekter/api/v4"
NVDB_V4_OMRADER_KONTRAKT_URL = "https://nvdbapiles.atlas.vegvesen.no/omrader/api/v4/kontraktsomrader"
NVDB_EKSPORT = "https://nvdb-eksport.atlas.vegvesen.no"


@dataclass(frozen=True)
class BBox:
    minx: float
    miny: float
    maxx: float
    maxy: float

    def width(self) -> float:
        return self.maxx - self.minx

    def height(self) -> float:
        return self.maxy - self.miny

    def split4(self) -> List["BBox"]:
        mx = (self.minx + self.maxx) / 2.0
        my = (self.miny + self.maxy) / 2.0
        return [
            BBox(self.minx, self.miny, mx, my),
            BBox(mx, self.miny, self.maxx, my),
            BBox(self.minx, my, mx, self.maxy),
            BBox(mx, my, self.maxx, self.maxy),
        ]

    def as_param(self) -> str:
        return f"{self.minx},{self.miny},{self.maxx},{self.maxy}"


def _dbg(msg: str) -> None:
    print(msg, flush=True)


def _print_http_error(r: requests.Response, prefix: str = "") -> None:
    _dbg(f"{prefix}HTTP {r.status_code} {r.reason}")
    _dbg(f"{prefix}URL: {r.url}")
    try:
        _dbg(f"{prefix}JSON: {r.json()}")
    except Exception:
        _dbg(f"{prefix}TEXT:\n{(r.text or '')[:2000]}")


def kontraktsnavn_fra_nummer(kontrakt_nummer: int, *, x_client: str, timeout: int = 120) -> str:
    headers = {"Accept": "application/json", "X-Client": x_client}
    r = requests.get(NVDB_V4_OMRADER_KONTRAKT_URL, headers=headers, timeout=timeout)
    if r.status_code >= 400:
        _print_http_error(r, prefix="[omrader] ")
        r.raise_for_status()

    data = r.json()
    if not isinstance(data, list):
        raise RuntimeError(f"Forventet liste fra {NVDB_V4_OMRADER_KONTRAKT_URL}, fikk {type(data)}")

    for item in data:
        if isinstance(item, dict) and item.get("nummer") == kontrakt_nummer and item.get("navn"):
            return str(item["navn"])

    raise RuntimeError(f"Fant ikke kontraktsområde med nummer={kontrakt_nummer} i Områder API.")


def normalize_kontrakt(arg: str, *, x_client: str) -> str:
    """
    Hvis --kontrakt er bare tall (f.eks 1509): slå opp kontraktsnavn og returner navn.
    Ellers: returner som gitt (antar at det allerede er navn).
    """
    s = (arg or "").strip()
    if re.fullmatch(r"\d+", s):
        navn = kontraktsnavn_fra_nummer(int(s), x_client=x_client)
        return navn
    return s


def _is_probably_html(blob: bytes) -> bool:
    h = blob[:400].lstrip().lower()
    return h.startswith(b"<html") or h.startswith(b"<!doctype html") or b"<head" in h[:400]


def _is_probably_sosi(blob: bytes) -> bool:
    head = blob[:6000]
    return (b".HODE" in head) or (b".PUNKT" in head) or (b".KURVE" in head) or (b".FLATE" in head)


def _parse_wkt_numbers(wkt: str) -> List[Tuple[float, float]]:
    nums = re.findall(r"(-?\d+(?:\.\d+)?)\s+(-?\d+(?:\.\d+)?)", wkt)
    pts: List[Tuple[float, float]] = []
    for a, b in nums:
        try:
            pts.append((float(a), float(b)))
        except ValueError:
            pass
    return pts


def _bbox_from_points(pts: List[Tuple[float, float]]) -> Optional[BBox]:
    if not pts:
        return None
    xs = [p[0] for p in pts]
    ys = [p[1] for p in pts]
    return BBox(min(xs), min(ys), max(xs), max(ys))


def fetch_bbox_v4(
    type_id: int,
    kontraktsnavn: str,
    vegsystemreferanse: str,
    *,
    x_client: str,
    page_size: int = 800,
    max_pages: int = 9999,
    sleep_s: float = 0.0,
) -> BBox:
    """
    Leser v4 paginert og bygger bbox fra geometri-wkt hvis tilgjengelig.
    NB: kontraktsnavn må være navnstreng (ikke bare nummer).
    """
    url = f"{NVDB_V4}/vegobjekter/{type_id}"
    headers = {"X-Client": x_client, "Accept": "application/json"}

    params: Optional[Dict[str, Any]] = {
        "kontraktsomrade": kontraktsnavn,
        "vegsystemreferanse": vegsystemreferanse,
        "inkluder": "lokasjon,geometri",
        "antall": str(page_size),
        "inkluderAntall": "false",
    }

    bbox: Optional[BBox] = None
    next_url: Optional[str] = url
    next_params: Optional[Dict[str, Any]] = params

    empty_pages = 0

    for page in range(1, max_pages + 1):
        if next_url is None:
            break

        r = requests.get(next_url, headers=headers, params=next_params, timeout=120)
        if r.status_code >= 400:
            _print_http_error(r, prefix="[v4] ")
            r.raise_for_status()

        data = r.json()
        objs = data.get("objekter") or []
        if not isinstance(objs, list):
            objs = []

        returned = len(objs)
        _dbg(f"v4 side {page}: returnert={returned}")

        if returned == 0:
            empty_pages += 1
            if empty_pages >= 1:
                break
        else:
            empty_pages = 0

        for o in objs:
            if not isinstance(o, dict):
                continue

            geom = o.get("geometri") or {}
            wkt = None
            if isinstance(geom, dict):
                wkt = geom.get("wkt")
            elif isinstance(geom, str):
                wkt = geom

            pts: List[Tuple[float, float]] = []
            if wkt:
                pts = _parse_wkt_numbers(wkt)

            b = _bbox_from_points(pts)
            if b:
                if bbox is None:
                    bbox = b
                else:
                    bbox = BBox(
                        min(bbox.minx, b.minx),
                        min(bbox.miny, b.miny),
                        max(bbox.maxx, b.maxx),
                        max(bbox.maxy, b.maxy),
                    )

        meta = data.get("metadata") or {}
        nxt = meta.get("neste")
        if isinstance(nxt, dict) and nxt.get("href"):
            next_url = nxt["href"]
            next_params = None
        else:
            break

        if sleep_s:
            time.sleep(sleep_s)

    if bbox is None:
        raise RuntimeError("Klarte ikke å beregne bbox fra v4 (mangler geometri i responsen).")

    margin = 1000.0
    return BBox(bbox.minx - margin, bbox.miny - margin, bbox.maxx + margin, bbox.maxy + margin)


def export_sosi_for_bbox(
    type_id: int,
    kontraktsnavn: str,
    vegsystemreferanse: str,
    bbox: BBox,
    *,
    x_client: str,
    timeout: int = 600,
    retries: int = 5,
) -> bytes:
    """
    Eksporter SOSI med kartutsnitt (filter!). sosiutsnitt settes lik bbox for header.
    """
    url = f"{NVDB_EKSPORT}/vegobjekter/{type_id}.sos"
    headers = {"X-Client": x_client, "Accept": "text/plain"}

    params = {
        "kontraktsomrade": kontraktsnavn,
        "vegsystemreferanse": vegsystemreferanse,
        "inkluder": "alle",
        "kartutsnitt": bbox.as_param(),
        "sosiutsnitt": bbox.as_param(),
    }

    # enkel retry/backoff for WAF/ustabilitet
    last_exc: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, headers=headers, params=params, timeout=timeout)
            if r.status_code >= 400:
                _print_http_error(r, prefix="[eksport] ")
                r.raise_for_status()

            blob = r.content
            if _is_probably_html(blob) or not _is_probably_sosi(blob):
                # ofte WAF/feilside med 200 OK
                head = blob[:600].decode("utf-8", errors="replace")
                raise RuntimeError(
                    f"Eksport returnerte ikke SOSI (attempt {attempt}). "
                    f"Content-Type={r.headers.get('Content-Type')}. Head:\n{head}"
                )
            return blob
        except Exception as e:
            last_exc = e
            sleep_s = min(5.0, 0.5 * (2 ** (attempt - 1)))
            _dbg(f"[eksport] forsøk {attempt}/{retries} feilet: {e}. Sover {sleep_s:.1f}s")
            time.sleep(sleep_s)

    raise RuntimeError(f"Eksport feilet etter {retries} forsøk. Siste feil: {last_exc}")


def write_bytes(path: str, blob: bytes) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        f.write(blob)


def quadtree_export(
    type_id: int,
    kontraktsnavn: str,
    vegsystemreferanse: str,
    root_bbox: BBox,
    out_dir: str,
    *,
    x_client: str,
    target_bytes: int,
    max_depth: int = 12,
    min_size_m: float = 200.0,
) -> List[str]:
    written: List[str] = []
    queue: List[Tuple[BBox, int]] = [(root_bbox, 0)]
    tile_no = 0

    while queue:
        bbox, depth = queue.pop(0)
        tile_no += 1

        _dbg(f"[tile {tile_no}] depth={depth} bbox={bbox.as_param()}")
        blob = export_sosi_for_bbox(
            type_id, kontraktsnavn, vegsystemreferanse, bbox, x_client=x_client
        )
        size = len(blob)

        if size == 0:
            _dbg(f"[tile {tile_no}] tom fil, hopper over")
            continue

        if size <= target_bytes:
            fn = os.path.join(out_dir, f"type{type_id}_tile{tile_no:04d}.sos")
            write_bytes(fn, blob)
            _dbg(f"[tile {tile_no}] OK {size/(1024*1024):.2f} MB -> {os.path.basename(fn)}")
            written.append(fn)
            continue

        if depth >= max_depth or (bbox.width() <= min_size_m and bbox.height() <= min_size_m):
            fn = os.path.join(out_dir, f"type{type_id}_tile{tile_no:04d}_FOR_STOR_{size}.sos")
            write_bytes(fn, blob)
            _dbg(f"[tile {tile_no}] ADVARSEL: fortsatt for stor ({size/(1024*1024):.2f} MB)")
            written.append(fn)
            continue

        _dbg(f"[tile {tile_no}] For stor ({size/(1024*1024):.2f} MB). Splitter i 4 ...")
        for child in bbox.split4():
            queue.append((child, depth + 1))

    return written


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--type", type=int, default=96)
    ap.add_argument("--kontrakt", required=True, help="Kontraktsområde: nummer (1509) eller navn ('1509 ...')")
    ap.add_argument("--vegsystem", default="FV")
    ap.add_argument("--out", default="out_sosi")
    ap.add_argument("--target-mb", type=float, default=4.5)
    ap.add_argument("--x-client", default="MRFK-DF20-export/1.0")
    ap.add_argument("--max-depth", type=int, default=12)
    args = ap.parse_args()

    kontraktsnavn = normalize_kontrakt(args.kontrakt, x_client=args.x_client)
    _dbg(f"Kontraktsområde brukt i API: {kontraktsnavn!r}")

    target_bytes = int(args.target_mb * 1024 * 1024)

    _dbg("Finner grovt kartutsnitt (bbox) via NVDB API Les v4 ...")
    bbox = fetch_bbox_v4(args.type, kontraktsnavn, args.vegsystem, x_client=args.x_client)
    _dbg(f"BBox (m/ margin): {bbox.as_param()}")

    _dbg("Eksporterer SOSI via NVDB Eksport og splitter på kartutsnitt ...")
    files = quadtree_export(
        args.type,
        kontraktsnavn,
        args.vegsystem,
        bbox,
        args.out,
        x_client=args.x_client,
        target_bytes=target_bytes,
        max_depth=args.max_depth,
    )

    _dbg(f"Ferdig. Skrev {len(files)} filer i: {os.path.abspath(args.out)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())