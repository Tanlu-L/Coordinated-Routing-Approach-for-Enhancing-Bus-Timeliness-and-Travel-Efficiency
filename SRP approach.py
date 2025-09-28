import os
import sys
import argparse
import subprocess
import shutil
import xml.etree.ElementTree as ET
import csv
import json


def parse_args():
    p = argparse.ArgumentParser(
        description="Run SUMO (SRP) and save per-stop bus arrivals (stopinfo.xml + stop_events.csv)."
    )
    p.add_argument("--cfg", required=True, help="Path to .sumocfg")
    p.add_argument("--outdir", required=True, help="Output folder (created if missing)")
    p.add_argument("--gui", action="store_true", help="Use sumo-gui instead of sumo")
    p.add_argument("--sumo-bin", default=None,
                   help="Path to sumo or sumo-gui binary. If not set, uses one from PATH.")
    p.add_argument("--", dest="extra", nargs=argparse.REMAINDER,
                   help="Extra flags passed to SUMO, e.g. -- --begin 0 --end 10800")
    p.add_argument("--only-extract", action="store_true",
                   help="Only extract stopinfo.xml -> stop_events.csv")
    return p.parse_args()

def which(exe_name):
    return shutil.which(exe_name)

def ensure_outdir(d):
    os.makedirs(d, exist_ok=True)

# Lauch sumo

def build_cmd(args, stopinfo_xml):
    # choose binary
    if args.sumo_bin:
        sumo_bin = args.sumo_bin
    else:
        prefer = "sumo-gui" if args.gui else "sumo"
        found = which(prefer)
        if found:
            sumo_bin = found
        else:
            alt = "sumo" if args.gui else "sumo-gui"
            alt_found = which(alt)
            if alt_found:
                sumo_bin = alt_found
            else:
                sys.stderr.write(
                    f"ERROR: Could not find '{prefer}' in PATH. "
                    f"Install SUMO or pass --sumo-bin /path/to/sumo\n"
                )
                sys.exit(1)

    cmd = [sumo_bin, "-c", args.cfg]
    cmd += ["--stop-output", stopinfo_xml]
    cmd += ["--duration-log.statistics", "true"]

    if args.extra:
        cmd += args.extra

    return cmd

def run_sumo(cmd):
    print("Launching SUMO with:\n  " + " ".join(cmd))
    proc = subprocess.run(cmd)
    if proc.returncode != 0:
        sys.stderr.write(f"SUMO exited with code {proc.returncode}\n")
        sys.exit(proc.returncode)

# Extractor

def _to_float_or_nan(v):
    if v is None:
        return float("nan")
    s = str(v).strip()
    if s == "" or s.lower() in ("nan", "none"):
        return float("nan")
    try:
        f = float(s)
        # SUMO sometimes uses negative for "not set"
        if f < 0:
            return float("nan")
        return f
    except Exception:
        return float("nan")

def _get_first_attr(el, names):
    """Return the first present attribute among 'names'; checks exact and case-insensitive."""
    for n in names:
        if n in el.attrib:
            return el.attrib.get(n)
    # case-insensitive fallback
    lowmap = {k.lower(): k for k in el.attrib.keys()}
    for n in names:
        k = lowmap.get(n.lower())
        if k:
            return el.attrib.get(k)
    return None

def extract_stopinfo_to_csv(stopinfo_xml, out_csv, debug_json=None, sample_n=10):
    """
    Convert SUMO's stopinfo.xml into a tidy CSV with:
      veh, stop, arrival_s, departure_s, dwell_s

    Handles attribute variants:
      stop id    : busStop | stop
      arrival    : arrival | started
      departure  : depart | departure | ended
    """
    rows = []
    debug_rows = []

    if not os.path.isfile(stopinfo_xml):
        sys.stderr.write(f"[WARN] stopinfo.xml not found at {stopinfo_xml}. No stop events to extract.\n")
    else:
        try:
            tree = ET.parse(stopinfo_xml)
            root = tree.getroot()
            has_any = False
            for el in root.iter():
                if el.tag != "stopinfo":
                    continue
                has_any = True

                # capture raw for debugging
                if debug_json and len(debug_rows) < sample_n:
                    debug_rows.append(dict(el.attrib))

                veh  = _get_first_attr(el, ["id", "vehID", "vehicle"]) or ""
                stop = _get_first_attr(el, ["busStop", "stop"]) or ""

                arr_str = _get_first_attr(el, ["arrival", "started"])
                dep_str = _get_first_attr(el, ["depart", "departure", "ended"])

                arr = _to_float_or_nan(arr_str)
                dep = _to_float_or_nan(dep_str)
                dwell = dep - arr if (arr == arr and dep == dep) else float("nan")

                rows.append((veh, stop, arr, dep, dwell))

            if not has_any:
                sys.stderr.write("[WARN] stopinfo.xml parsed but contained 0 <stopinfo> rows.\n")
        except ET.ParseError as e:
            sys.stderr.write(f"[WARN] Could not parse {stopinfo_xml}: {e}\n")

    # Write CSV
    os.makedirs(os.path.dirname(out_csv) or ".", exist_ok=True)
    with open(out_csv, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["veh", "stop", "arrival_s", "departure_s", "dwell_s"])
        w.writerows(rows)

    # Debug sample
    if debug_json:
        try:
            with open(debug_json, "w") as jf:
                json.dump(debug_rows, jf, indent=2)
            print(f"[debug] wrote first {len(debug_rows)} raw rows to {debug_json}")
        except Exception as e:
            sys.stderr.write(f"[WARN] could not write debug json: {e}\n")

    n_rows = len(rows)
    n_unique_stops = len({r[1] for r in rows}) if rows else 0
    n_unique_veh = len({r[0] for r in rows}) if rows else 0
    print(f"Wrote {out_csv}  (rows: {n_rows}, buses: {n_unique_veh}, stops: {n_unique_stops})")

# Main

def main():
    args = parse_args()
    ensure_outdir(args.outdir)

    stopinfo_xml = os.path.join(args.outdir, "stopinfo.xml")
    stop_events_csv = os.path.join(args.outdir, "stop_events.csv")
    debug_json = os.path.join(args.outdir, "stopinfo_debug_sample.json")

    if not args.only_extract:
        cmd = build_cmd(args, stopinfo_xml)
        run_sumo(cmd)

    # Post-process the NEW data only (does not touch your existing outputs)
    extract_stopinfo_to_csv(stopinfo_xml, stop_events_csv, debug_json=debug_json)

    print("Done.")

if __name__ == "__main__":
    main()

