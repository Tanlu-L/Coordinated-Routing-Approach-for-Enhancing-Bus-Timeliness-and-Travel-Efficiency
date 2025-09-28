import os
import sys
import csv
import argparse
import shutil
import subprocess
import xml.etree.ElementTree as ET
from collections import defaultdict

# Traci
import traci
import traci.constants as tc

def parse_args():
    p = argparse.ArgumentParser(
        description="Run SUMO with reactive CAV rerouting (every 60s) and log per-vehicle and bus stop data."
    )
    p.add_argument("--cfg", required=True, help="Path to .sumocfg")
    p.add_argument("--outdir", required=True, help="Output directory")
    p.add_argument("--gui", action="store_true", help="Use sumo-gui")
    p.add_argument("--sumo-bin", default=None, help="Path to sumo/sumo-gui; else search PATH")
    p.add_argument("--step-length", type=float, default=1.0, help="SUMO step length (s)")
    p.add_argument("--end", type=float, default=None, help="Force end time (s), overrides .sumocfg")
    # Behavior knobs
    p.add_argument("--cav-class", default="taxi", help="CAV vClass (your t_0 uses vClass='taxi')")
    p.add_argument("--bus-key", default="f_0", help="Substring to detect bus IDs in addition to vClass=bus")
    p.add_argument("--reroute-period", type=float, default=60.0, help="Seconds between CAV reroutes")
    p.add_argument("--bus-dwell-s", type=float, default=60.0, help="Planned dwell seconds per stop for free-flow")
    p.add_argument("--bus-nstops", type=int, default=3, help="Number of scheduled stops for bus free-flow baseline")
    
    # Extraction 
    p.add_argument("--only-extract", action="store_true",
                   help="Skip SUMO; parse outdir/stopinfo.xml -> stop_events.csv")
    return p.parse_args()

def which(exe): return shutil.which(exe)
def ensure_dir(d): os.makedirs(d, exist_ok=True)

def build_sumo_cmd(args, stopinfo_xml):
    if args.sumo_bin:
        bin_path = args.sumo_bin
    else:
        pref = "sumo-gui" if args.gui else "sumo"
        found = which(pref) or which("sumo") or which("sumo-gui")
        if not found:
            sys.exit(f"ERROR: cannot find sumo/sumo-gui in PATH; try --sumo-bin")
        bin_path = found

    cmd = [bin_path, "-c", args.cfg,
           "--duration-log.statistics", "true",
           "--stop-output", stopinfo_xml,
           "--step-length", str(args.step_length)]
    if args.end is not None:
        cmd += ["--end", str(int(args.end))]
    return cmd

# stop info
def _to_float_or_nan(v):
    if v is None: return float("nan")
    s = str(v).strip()
    if s == "" or s.lower() in ("nan", "none"): return float("nan")
    try:
        f = float(s)
        return f if f >= 0 else float("nan")
    except Exception:
        return float("nan")

def _get_attr(el, names):
    for n in names:
        if n in el.attrib:
            return el.attrib[n]
    low = {k.lower(): k for k in el.attrib}
    for n in names:
        k = low.get(n.lower())
        if k: return el.attrib[k]
    return None

def parse_stopinfo_xml(stopinfo_xml):
    rows = []
    if not os.path.isfile(stopinfo_xml):
        return rows
    try:
        root = ET.parse(stopinfo_xml).getroot()
        for el in root.iter("stopinfo"):
            vid  = _get_attr(el, ["id", "vehID", "vehicle"]) or ""
            stop = _get_attr(el, ["busStop", "stop"]) or ""
            arr  = _to_float_or_nan(_get_attr(el, ["arrival", "started"]))
            dep  = _to_float_or_nan(_get_attr(el, ["depart", "departure", "ended"]))
            dwell = dep - arr if (arr == arr and dep == dep) else float("nan")
            rows.append((vid, stop, arr, dep, dwell))
    except ET.ParseError:
        pass
    return rows

def write_csv(rows, out_csv, header):
    ensure_dir(os.path.dirname(out_csv) or ".")
    with open(out_csv, "w", newline="") as f:
        w = csv.writer(f)
        if header:
            w.writerow(header)
        w.writerows(rows)
    print(f"[write] {out_csv} ({len(rows)} rows)")

#stop-
class StopLogger:
    """Record bus stop arrivals/departures each step via TraCI."""
    def __init__(self, is_bus_fn):
        self.is_bus = is_bus_fn
        self.active = {}  # vid -> (stop_id, arrival_s)
        self.rows = []    # (veh, stop, arrival_s, departure_s, dwell_s)

    def step(self, t):
        for vid in traci.vehicle.getIDList():
            if not self.is_bus(vid):
                continue
            try:
                stop_id = traci.vehicle.getStopID(vid) or ""
            except Exception:
                stop_id = ""
            if vid not in self.active:
                if stop_id:
                    self.active[vid] = (stop_id, float(t))
                else:
                    self.active[vid] = (None, None)
            else:
                prev, arr_t = self.active[vid]
                if prev != stop_id:
                    if prev and not stop_id:
                        dep_t = float(t)
                        dwell = dep_t - (arr_t if arr_t is not None else dep_t)
                        self.rows.append((vid, prev, arr_t or dep_t, dep_t, dwell))
                        self.active[vid] = (None, None)
                    elif not prev and stop_id:
                        self.active[vid] = (stop_id, float(t))
                    else:
                        # stop->stop jump
                        if prev:
                            dep_t = float(t)
                            dwell = dep_t - (arr_t if arr_t is not None else dep_t)
                            self.rows.append((vid, prev, arr_t or dep_t, dep_t, dwell))
                        self.active[vid] = (stop_id, float(t))

    def finalize(self, t):
        for vid, (stop_id, arr_t) in list(self.active.items()):
            if stop_id:
                dep_t = float(t)
                dwell = dep_t - (arr_t if arr_t is not None else dep_t)
                self.rows.append((vid, stop_id, arr_t or dep_t, dep_t, dwell))
        return list(self.rows)

class VehicleLogger:
    """
    Tracks depart/arrive times and logs:
    veh, class, is_cav, depart_s, arrive_s, travel_s, delay_s
    where delay_s = travel_s - freeflow_s
    """
    def __init__(self, cav_class, bus_dwell_s=60.0, bus_nstops=3):
        self.cav_class = cav_class
        self.bus_dwell_s = float(bus_dwell_s)
        self.bus_nstops = int(bus_nstops)

        self.depart_t = {}     # vid -> depart_s
        self.vclass   = {}     # vid -> class at depart
        self.is_cav   = {}     # vid -> bool
        self.freeflow = {}     # vid -> s
        self.rows     = []     # list of output rows

    def _edge_freeflow_time(self, edge_id):
        try:
            nlanes = traci.edge.getLaneNumber(edge_id)
            if nlanes <= 0:
                return 0.0
            speeds = []
            for i in range(nlanes):
                lane_id = f"{edge_id}_{i}"
                try:
                    speeds.append(traci.lane.getMaxSpeed(lane_id))
                except Exception:
                    pass
            vmax = max(speeds) if speeds else 0.0
            length = traci.edge.getLength(edge_id)
            return (length / vmax) if vmax > 0 else 0.0
        except Exception:
            return 0.0

    def _route_freeflow_time(self, route_edges, is_bus=False):
        t = sum(self._edge_freeflow_time(e) for e in route_edges)
        if is_bus:
            t += self.bus_dwell_s * self.bus_nstops
        return t

    def on_depart(self, vid, t):
        try:
            vcls = traci.vehicle.getVehicleClass(vid)
        except Exception:
            vcls = ""
        self.depart_t[vid] = float(t)
        self.vclass[vid] = vcls
        self.is_cav[vid] = (vcls == self.cav_class)

        # compute free-flow baseline from current route
        try:
            route_edges = traci.vehicle.getRoute(vid) or []
        except Exception:
            route_edges = []
        is_bus = (vcls == "bus")
        self.freeflow[vid] = self._route_freeflow_time(route_edges, is_bus=is_bus)

    def on_arrive(self, vid, t):
        if vid not in self.depart_t:
            return None
        depart_s = self.depart_t.pop(vid)
        arrive_s = float(t)
        travel_s = arrive_s - depart_s
        ff = self.freeflow.pop(vid, float("nan"))
        delay_s = (travel_s - ff) if (ff == ff) else float("nan")
        row = (
            vid,
            self.vclass.pop(vid, ""),
            1 if self.is_cav.pop(vid, False) else 0,
            depart_s,
            arrive_s,
            travel_s,
            delay_s,
        )
        self.rows.append(row)
        return row

def write_run_summary(per_vehicle_rows, out_csv):
    agg = defaultdict(lambda: {"n":0, "sum_travel":0.0, "sum_delay":0.0})
    for (_, vcls, _, _, _, travel_s, delay_s) in per_vehicle_rows:
        c = vcls if vcls else "unknown"
        agg[c]["n"] += 1
        agg[c]["sum_travel"] += float(travel_s)
        if delay_s == delay_s:
            agg[c]["sum_delay"] += float(delay_s)
    rows = [("class","n_finished","mean_travel_min","mean_delay_min","total_delay_min")]
    for cls, v in sorted(agg.items()):
        n = v["n"]
        mean_travel_min = (v["sum_travel"]/n/60.0) if n>0 else float("nan")
        mean_delay_min  = (v["sum_delay"]/n/60.0) if n>0 else float("nan")
        total_delay_min = v["sum_delay"]/60.0
        rows.append((cls, n, f"{mean_travel_min:.3f}", f"{mean_delay_min:.3f}", f"{total_delay_min:.3f}"))
    write_csv(rows, out_csv, header=[])

# Main
def run(args):
    ensure_dir(args.outdir)

    stopinfo_xml = os.path.join(args.outdir, "stopinfo.xml")
    f_stop = os.path.join(args.outdir, "stop_events.csv")
    f_per  = os.path.join(args.outdir, "per_vehicle.csv")
    f_sum  = os.path.join(args.outdir, "run_summary.csv")

    if args.only_extract:
        rows = parse_stopinfo_xml(stopinfo_xml)
        write_csv(rows, f_stop, ["veh","stop","arrival_s","departure_s","dwell_s"])
        print("Done (extraction only).")
        return

    cmd = build_sumo_cmd(args, stopinfo_xml)

    if traci is None or tc is None:
        print("[INFO] TraCI not available; running SUMO without control (no per-vehicle metrics).")
        rc = subprocess.run(cmd).returncode
        if rc != 0:
            sys.exit(f"SUMO exited with code {rc}")
        rows = parse_stopinfo_xml(stopinfo_xml)
        write_csv(rows, f_stop, ["veh","stop","arrival_s","departure_s","dwell_s"])
        print("[INFO] per_vehicle/run_summary require TraCI; skipped.")
        print("Done.")
        return

    is_bus = lambda vid: (args.bus_key in vid) or (traci.vehicle.getVehicleClass(vid) == "bus")
    stop_logger = StopLogger(is_bus)
    veh_logger  = VehicleLogger(args.cav_class, args.bus_dwell_s, args.bus_nstops)

    traci.start(cmd)
    try:
        last_reroute_slot = -1
        last_print_min = -1

        # Optional: make sure SUMO isn't auto-rerouting; we control it manually
        # (safe even if the parameter is absent)
        try:
            traci.simulation.setParameter("device.rerouting.probability", "0")
        except Exception:
            pass

        while traci.simulation.getMinExpectedNumber() > 0:
            traci.simulationStep()
            t = traci.simulation.getTime()

            # Depart
            for vid in traci.simulation.getDepartedIDList():
                veh_logger.on_depart(vid, t)

            # Periodic CAV reroute
            slot = int(t // args.reroute_period)
            if slot != last_reroute_slot and t > 0:
                last_reroute_slot = slot
                active = traci.vehicle.getIDList()
                for vid in active:
                    try:
                        if veh_logger.is_cav.get(vid, False):
                            traci.vehicle.rerouteTraveltime(vid)
                    except Exception:
                        pass

            for vid in traci.simulation.getArrivedIDList():
                veh_logger.on_arrive(vid, t)

            stop_logger.step(t)

            if int(t)//60 != last_print_min:
                last_print_min = int(t)//60
                print(f"[sim] t={int(t)}s, remaining={traci.simulation.getMinExpectedNumber()}")

    finally:
        t_final = traci.simulation.getTime()
        traci.close(False)

    # Stop events: prefer TraCI rows; fallback to XML if empty
    stop_rows = stop_logger.finalize(t_final)
    if not stop_rows:
        stop_rows = parse_stopinfo_xml(stopinfo_xml)

    # Save results
    write_csv(stop_rows, f_stop, ["veh","stop","arrival_s","departure_s","dwell_s"])
    write_csv(veh_logger.rows, f_per, ["veh","class","is_cav","depart_s","arrive_s","travel_s","delay_s"])
    write_run_summary(veh_logger.rows, f_sum)

    print("Done.")

def main():
    args = parse_args()
    run(args)

if __name__ == "__main__":
    main()
