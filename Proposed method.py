import os, sys, csv, math, argparse, shutil, subprocess, json
import xml.etree.ElementTree as ET
from collections import defaultdict, deque
try:
    import traci
    import traci.constants as tc
except Exception:
    traci = None
    tc = None

def parse_args():
    p = argparse.ArgumentParser(
        description="proposed method code"
    )
    p.add_argument("--cfg", help="Path to .sumocfg")
    p.add_argument("--rou", action="append",
                   help="Path to a *.rou.xml")
    p.add_argument("--additional", default=None,
                   help="Comma/semicolon-separated additional files ")
    p.add_argument("--outdir", required=True, help="Output directory")
    p.add_argument("--gui", action="store_true", help="Use sumo-gui (default: sumo)")
    p.add_argument("--sumo-bin", default=None, help="Path to sumo/sumo-gui; if unset, search PATH")
    p.add_argument("--step-length", type=float, default=1.0, help="SUMO step length (s)")
    p.add_argument("--begin", type=float, default=None, help="Override begin time (s)")
    p.add_argument("--end", type=float, default=None, help="Override end time (s)")
    p.add_argument("--seed", type=int, default=42, help="SUMO random seed")
    p.add_argument("--no-teleport-bus", action="store_true",
                   help="Make teleport effectively disabled; safer for bus schedule metrics")
    p.add_argument("--tripinfo", action="store_true",
                   help="Write tripinfo.xml")
    p.add_argument("--cav-class", default="taxi", help="CAV vClass")
    p.add_argument("--bus-key", default="f_0", help="Substring identifying bus IDs")
    p.add_argument("--dbl-lanes", default="",
                   help="Semicolon- or comma-separated DL identifiers. "\
                        "Example: 'edgeA_0;edgeB_0;edgeC'")
    p.add_argument("--delta", type=float, default=30.0, help="DL monitor window")
    p.add_argument("--delta-gpl", type=float, default=60.0, help="GPL monitor window")
    p.add_argument("--alpha-dl", type=float, default=0.2)
    p.add_argument("--beta-dl",  type=float, default=5.0)
    p.add_argument("--alpha-gpl", type=float, default=0.1)
    p.add_argument("--beta-gpl",  type=float, default=3.0)
    p.add_argument("--cap-dl", type=float, default=800.0, help="DL effective capacity")
    p.add_argument("--cap-gpl", type=float, default=600.0, help="GPL effective capacity")
    p.add_argument("--no-control", action="store_true", help="Disable controller; just log data")
    p.add_argument("--only-extract", action="store_true",
                   help="Skip running SUMO; just parse outdir/stopinfo.xml -> stop_events.csv")
    p.add_argument("--", dest="extra", nargs=argparse.REMAINDER,
                   help="Extra flags passed to SUMO, e.g. -- --device.rerouting.probability 0")
    return p.parse_args()

def which(exe): return shutil.which(exe)
def ensure_dir(d): os.makedirs(d, exist_ok=True)
def write_csv(rows, out_csv, header=None):
    os.makedirs(os.path.dirname(out_csv) or ".", exist_ok=True)
    with open(out_csv, "w", newline="") as f:
        w = csv.writer(f)
        if header is not None:
            w.writerow(header)
        w.writerows(rows)

# stop info
def _to_float_or_nan(v):
    if v is None: return float("nan")
    s = str(v).strip()
    if s == "" or s.lower() in ("nan", "none"):
        return float("nan")
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
        if k:
            return el.attrib[k]
    return None

def parse_stopinfo_xml(stopinfo_xml):
    rows = []
    if not os.path.isfile(stopinfo_xml):
        return rows
    try:
        tree = ET.parse(stopinfo_xml)
        root = tree.getroot()
        for el in root.iter():
            if el.tag != "stopinfo":
                continue
            veh  = _get_attr(el, ["id", "vehID", "vehicle"]) or ""
            stop = _get_attr(el, ["busStop", "stop"]) or ""
            arr_str = _get_attr(el, ["arrival", "started"])
            dep_str = _get_attr(el, ["depart", "departure", "ended"])
            arr = _to_float_or_nan(arr_str)
            dep = _to_float_or_nan(dep_str)
            dwell = dep - arr if (arr == arr and dep == dep) else float("nan")
            rows.append((veh, stop, arr, dep, dwell))
    except ET.ParseError:
        pass
    return rows

# track bus arrive/departure
class StopLogger:
    def __init__(self, is_bus_fn):
        self.is_bus = is_bus_fn
        self.active = {}  
        self.rows = []

    def step(self, t):
        try:
            vids = traci.vehicle.getIDList()
        except Exception:
            vids = []
        for vid in vids:
            if not self.is_bus(vid):
                continue
            stop_id = ""
            try:
                stop_id = traci.vehicle.getNextStop(vid)[0] 
            except Exception:
                pass

            prev, arr_t = self.active.get(vid, (None, None))
            if prev and prev != stop_id:
                dep_t = float(t)
                dwell = dep_t - (arr_t if arr_t is not None else dep_t)
                self.rows.append((vid, prev, arr_t or dep_t, dep_t, dwell))
                self.active[vid] = (None, None)
            elif not prev and stop_id:
                self.active[vid] = (stop_id, float(t))
            elif prev and stop_id and prev != stop_id:
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

# track per-veh behavior
class VehicleLogger:
    def __init__(self, cav_class):
        self.cav_class = cav_class
        self.depart = {}
        self.vclass = {}
        self.is_cav = {}
        self.timeloss = {}
        self.waiting = {}
        self.rows = []

    def on_depart(self, vid, t):
        try:
            vcls = traci.vehicle.getVehicleClass(vid)
        except Exception:
            vcls = ""
        self.depart[vid] = float(t)
        self.vclass[vid] = vcls
        self.is_cav[vid] = (vcls == self.cav_class)
        try:
            traci.vehicle.subscribe(vid, (tc.VAR_TIMELOSS, tc.VAR_ACCUMULATED_WAITING_TIME))
        except Exception:
            try:
                traci.vehicle.subscribe(vid, (tc.VAR_ACCUMULATED_WAITING_TIME,))
            except Exception:
                pass

    def on_step_collect(self, vid):
        try:
            res = traci.vehicle.getSubscriptionResults(vid)
        except Exception:
            res = None
        if res:
            if tc and tc.VAR_TIMELOSS in res:
                self.timeloss[vid] = float(res[tc.VAR_TIMELOSS])
            if tc and tc.VAR_ACCUMULATED_WAITING_TIME in res:
                self.waiting[vid] = float(res[tc.VAR_ACCUMULATED_WAITING_TIME])

    def on_arrive(self, vid, t):
        if vid not in self.depart:
            return None
        depart_s = self.depart.pop(vid)
        arrive_s = float(t)
        travel_s = arrive_s - depart_s
        delay_s = self.timeloss.pop(vid, self.waiting.pop(vid, 0.0))
        row = (
            vid,
            self.vclass.pop(vid, ""),
            1 if self.is_cav.pop(vid, False) else 0,
            depart_s,
            arrive_s,
            travel_s,
            float(delay_s) if delay_s is not None else float("nan"),
        )
        self.rows.append(row)
        return row

# write summary
def write_run_summary(per_vehicle_rows, out_csv):
    agg = defaultdict(lambda: {"n": 0, "sum_travel": 0.0, "sum_delay": 0.0})
    for (_, cls, _, _, _, travel_s, delay_s) in per_vehicle_rows:
        c = cls if cls else "unknown"
        agg[c]["n"] += 1
        agg[c]["sum_travel"] += float(travel_s)
        if delay_s == delay_s:  # not NaN
            agg[c]["sum_delay"] += float(delay_s)

    rows = [("class", "n_finished", "mean_travel_min", "mean_delay_min", "total_delay_min")]
    for cls, v in sorted(agg.items()):
        n = v["n"]
        mean_travel_min = (v["sum_travel"]/n/60.0) if n > 0 else float("nan")
        mean_delay_min  = (v["sum_delay"]/n/60.0) if n > 0 else float("nan")
        total_delay_min = v["sum_delay"]/60.0
        rows.append((cls, n, f"{mean_travel_min:.3f}", f"{mean_delay_min:.3f}", f"{total_delay_min:.3f}"))
    write_csv(rows, out_csv, header=[])

# Control
def _edge_from_lane(lid):
    try:
        return traci.lane.getEdgeID(lid)
    except Exception:
        return lid 

def _collect_dl_edges(args):
    dl = set()
    if not args.dbl_lanes:
        return dl
    tokens = []
    for tok in (args.dbl_lanes.replace(",", ";")).split(";"):
        tok = tok.strip()
        if tok:
            tokens.append(tok)
    for token in tokens:
        e = _edge_from_lane(token)
        dl.add(e)
    return dl

# free flow travel time
def _edge_tau0(edge_id):
    try:
        L = traci.edge.getLength(edge_id)
        lanes = traci.edge.getLanes(edge_id)
        vmax = 0.0
        for lid in lanes:
            vmax = max(vmax, traci.lane.getMaxSpeed(lid))
        if vmax <= 0:
            vmax = 13.89  
        return L / vmax
    except Exception:
        return 1.0 

# monitor 
class EdgeInflowMonitor:
    def __init__(self, window_s=30.0):
        self.window = float(window_s)
        self.prev_ids = defaultdict(set)         
        self.buf = defaultdict(lambda: deque()) 
        self.sum_in = defaultdict(float)       

    def step(self, t, edge_ids):
        for e in edge_ids:
            try:
                now_ids = set(traci.edge.getLastStepVehicleIDs(e))
            except Exception:
                now_ids = set()
            prev = self.prev_ids[e]
            inflow = len(now_ids - prev) 
            self.prev_ids[e] = now_ids

            dq = self.buf[e]
            dq.append((t, inflow))
            self.sum_in[e] += inflow

            while dq and dq[0][0] < t - self.window:
                _, c = dq.popleft()
                self.sum_in[e] -= c
                if self.sum_in[e] < 0:
                    self.sum_in[e] = 0.0

    def fhat(self, edge_id):
        if self.window <= 0:
            return 0.0
        return self.sum_in[edge_id] / self.window  

#BPR model (seperate GPL and DL)
class BPRModel:
    def __init__(self, dl_edges, alpha_dl=0.2, beta_dl=5.0, alpha_gpl=0.1, beta_gpl=3.0,
                 cap_dl=800.0, cap_gpl=600.0):
        self.dl_edges = set(dl_edges)
        self.alpha_dl = float(alpha_dl); self.beta_dl = float(beta_dl)
        self.alpha_gpl = float(alpha_gpl); self.beta_gpl = float(beta_gpl)
        self.cap_dl = float(cap_dl); self.cap_gpl = float(cap_gpl)
        self.tau0 = {}  

    def is_dl(self, e): return e in self.dl_edges

    def get_tau0(self, e):
        if e not in self.tau0:
            self.tau0[e] = _edge_tau0(e)
        return self.tau0[e]

    def cb(self, e):
        return self.cap_dl if self.is_dl(e) else self.cap_gpl

    def ab(self, e):
        return (self.alpha_dl, self.beta_dl) if self.is_dl(e) else (self.alpha_gpl, self.beta_gpl)

    def tauhat(self, e, fhat_hph):
        tau0 = self.get_tau0(e)
        c = self.cb(e)
        a, b = self.ab(e)
        if c <= 0:
            return tau0
        return tau0 * (1.0 + a * ((max(fhat_hph, 0.0) / c) ** b))

class BusHorizon:
    def __init__(self):
        self.active = {}  

    def update(self, bus_id, t):
        info = self.active.get(bus_id)
        if info and t > info["t_end"]:
            self.active.pop(bus_id, None)

    def maybe_open(self, bus_id, t):
        try:
            route = traci.vehicle.getRoute(bus_id)
            idx = traci.vehicle.getRouteIndex(bus_id)
            if idx < 0 or idx >= len(route):
                return None
            prevE = route[idx]
            if idx + 1 >= len(route):
                return None
            nextE = route[idx + 1]
        except Exception:
            return None

        key = self.active.get(bus_id)
        if not key or key["prevE"] != prevE or key["nextE"] != nextE:
            t_end = t + _edge_tau0(prevE)
            self.active[bus_id] = {"prevE": prevE, "nextE": nextE, "t_end": t_end}
        return self.active.get(bus_id)

class Rerouter:
    def __init__(self, bpr: BPRModel):
        self.bpr = bpr
        self.last_effort_time = None

    def push_efforts(self, t):
        if self.last_effort_time is not None and int(self.last_effort_time) == int(t):
            return
        for e in traci.edge.getIDList():
            try:
                traci.edge.setEffort(e, t, self.bpr.get_tau0(e))
            except Exception:
                pass
        self.last_effort_time = t

    def set_edge_effort(self, edge_id, t, value):
        try:
            traci.edge.setEffort(edge_id, t, float(value))
        except Exception:
            pass

    def find_and_set_route(self, veh_id, from_edge, to_edge, t):
        try:
            r = traci.simulation.findRoute(from_edge, to_edge,
                                           vType=traci.vehicle.getTypeID(veh_id))
            if r and r.edges:
                traci.vehicle.setRoute(veh_id, r.edges)
                return True
        except Exception:
            pass
        return False

# Control Hook (Equation 14 to 18 from paper)
def control_step(args, sim_time, vehlog, stoplog):
    if args.no_control:
        return

    # Initialize singletons on first call
    if not hasattr(control_step, "_init"):
        control_step._init = True
        control_step.dl_edges = _collect_dl_edges(args)
        control_step.bpr = BPRModel(
            control_step.dl_edges,
            alpha_dl=args.alpha_dl, beta_dl=args.beta_dl,
            alpha_gpl=args.alpha_gpl, beta_gpl=args.beta_gpl,
            cap_dl=args.cap_dl, cap_gpl=args.cap_gpl
        )
        control_step.inflow_dl  = EdgeInflowMonitor(window_s=args.delta)
        control_step.inflow_gpl = EdgeInflowMonitor(window_s=args.delta_gpl)
        control_step.busHZ = BusHorizon()
        control_step.rr = Rerouter(control_step.bpr)
        control_step.all_edges = list(traci.edge.getIDList())

    #inflow monitors
    control_step.inflow_dl.step(sim_time, control_step.all_edges)
    control_step.inflow_gpl.step(sim_time, control_step.all_edges)

    # bus
    bus_ids = []
    try:
        for vid in traci.vehicle.getIDList():
            if (args.bus_key and args.bus_key in vid) or traci.vehicle.getVehicleClass(vid) == "bus":
                bus_ids.append(vid)
    except Exception:
        pass

    for b in bus_ids:
        control_step.busHZ.update(b, sim_time)
        info = control_step.busHZ.maybe_open(b, sim_time)
        if not info:
            continue
        prevE, nextE, t_end = info["prevE"], info["nextE"], info["t_end"]

        # Predict \hat τ 
        fhat_next = control_step.inflow_dl.fhat(nextE)  
        tau0_next = control_step.bpr.get_tau0(nextE)
        tauhat    = control_step.bpr.tauhat(nextE, fhat_next * 3600.0)  

        # τ̂_b,k >= τ^0_b,k and within horizon
        if tauhat >= tau0_next and sim_time <= t_end:
            reroute_set = []
            try:
                vids_prev = traci.edge.getLastStepVehicleIDs(prevE)
            except Exception:
                vids_prev = []
            for vid in vids_prev:
                try:
                    if traci.vehicle.getVehicleClass(vid) != args.cav_class:
                        continue
                    lane_id = traci.vehicle.getLaneID(vid)
                    pos     = traci.vehicle.getLanePosition(vid)  # m
                    Llane   = traci.lane.getLength(lane_id)       # m
                    v       = max(traci.vehicle.getSpeed(vid), 0.1)  # m/s
                    eta     = max((Llane - pos) / v, 0.0)         # s to reach vb,k
                    if eta <= args.delta:
                        reroute_set.append(vid)
                except Exception:
                    continue

            if not reroute_set:
                continue

            # tauhat for all edges at t
            control_step.rr.push_efforts(sim_time)
            for e in control_step.all_edges:
                fhat_edge = (control_step.inflow_dl.fhat(e) if e in control_step.dl_edges
                             else control_step.inflow_gpl.fhat(e))
                control_step.rr.set_edge_effort(e, sim_time,
                                                control_step.bpr.tauhat(e, fhat_edge * 3600.0))

            BIG = 1e9
            control_step.rr.set_edge_effort(nextE, sim_time, BIG)

            # Reroute each CAV 
            for vid in reroute_set:
                try:
                    route = traci.vehicle.getRoute(vid)
                    if not route:
                        continue
                    destE = route[-1]
                    control_step.rr.find_and_set_route(vid, from_edge=prevE, to_edge=destE, t=sim_time)
                except Exception:
                    continue
            control_step.rr.set_edge_effort(nextE, sim_time, tauhat)

# Sumo cmd
def build_sumo_cmd(args, stopinfo_xml):
    if args.sumo_bin:
        bin_path = args.sumo_bin
    else:
        pref = "sumo-gui" if args.gui else "sumo"
        found = which(pref)
        if not found:
            alt = "sumo" if args.gui else "sumo-gui"
            found = which(alt)
            if not found:
                sys.exit(f"ERROR: cannot find '{pref}' or alternate SUMO binary in PATH")
        bin_path = found
    use_raw = bool(args.net or args.rou)
    if use_raw:
        if not args.net:
            sys.exit("RAW mode: --net is required.")
        if not args.rou:
            sys.exit("RAW mode: at least one --rou is required.")
    else:
        if not args.cfg:
            sys.exit("Provide either --cfg (CFG mode) OR --net and --rou (RAW mode).")

    cmd = [bin_path]
    if use_raw:
        cmd += ["-n", args.net, "-r", ",".join(args.rou)]
        if args.additional:
            add_files = args.additional.replace(";", ",")
            cmd += ["--additional-files", add_files]
    else:
        cmd += ["-c", args.cfg]

    cmd += [
        "--duration-log.statistics", "true",
        "--stop-output", stopinfo_xml,
        "--step-length", str(args.step_length),
        "--seed", str(args.seed),
        "--no-step-log", "true",
        "--log", os.path.join(args.outdir, "sumo.log"),
        "--error-log", os.path.join(args.outdir, "sumo_errors.log")
    ]
    if args.tripinfo:
        cmd += ["--tripinfo-output", os.path.join(args.outdir, "tripinfo.xml")]
    if args.no_teleport_bus:
        cmd += ["--time-to-teleport", "99999", "--time-to-teleport.bidi", "99999"]
    if args.begin is not None:
        cmd += ["--begin", str(int(args.begin))]
    if args.end is not None:
        cmd += ["--end", str(int(args.end))]
    if args.gui:
        cmd += ["--start", "true"]

    if args.extra:
        cmd += args.extra
    return cmd

# Main
def run(args):
    ensure_dir(args.outdir)

    stopinfo_xml = os.path.join(args.outdir, "stopinfo.xml")
    f_stop = os.path.join(args.outdir, "stop_events.csv")
    f_per  = os.path.join(args.outdir, "per_vehicle.csv")
    f_sum  = os.path.join(args.outdir, "run_summary.csv")

    if args.only_extract:
        rows = parse_stopinfo_xml(stopinfo_xml)
        write_csv(rows, f_stop, ["veh", "stop", "arrival_s", "departure_s", "dwell_s"])
        print("Done (extraction only).")
        return

    cmd = build_sumo_cmd(args, stopinfo_xml)

    if traci is None or tc is None:
        print("[INFO] TraCI not available; running SUMO without control.")
        rc = subprocess.run(cmd).returncode
        if rc != 0:
            sys.exit(f"SUMO exited with code {rc}")
        rows = parse_stopinfo_xml(stopinfo_xml)
        write_csv(rows, f_stop, ["veh", "stop", "arrival_s", "departure_s", "dwell_s"])
        print("[INFO] per_vehicle/run_summary require TraCI; skipped.")
        print("Done.")
        return

    # TraCI mode
    is_bus = lambda vid: (args.bus_key in vid) or (traci.vehicle.getVehicleClass(vid) == "bus")
    stop_logger = StopLogger(is_bus)
    veh_logger  = VehicleLogger(args.cav_class)

    # Save a small manifest
    manifest = {
        "mode": "RAW" if (args.net or args.rou) else "CFG",
        "cfg": args.cfg,
        "net": args.net,
        "rou": args.rou,
        "additional": args.additional,
        "outdir": args.outdir,
        "seed": args.seed,
        "begin": args.begin,
        "end": args.end,
        "delta": args.delta,
        "delta_gpl": args.delta_gpl,
        "alpha_dl": args.alpha_dl, "beta_dl": args.beta_dl,
        "alpha_gpl": args.alpha_gpl, "beta_gpl": args.beta_gpl,
        "cap_dl": args.cap_dl, "cap_gpl": args.cap_gpl,
        "dbl_lanes": args.dbl_lanes,
        "no_control": args.no_control
    }
    with open(os.path.join(args.outdir, "manifest.json"), "w") as mf:
        json.dump(manifest, mf, indent=2)

    traci.start(cmd)
    try:
        last_print_min = -1
        while traci.simulation.getMinExpectedNumber() > 0:
            traci.simulationStep()
            t = traci.simulation.getTime()

            for vid in traci.simulation.getDepartedIDList():
                veh_logger.on_depart(vid, t)

            for vid in traci.vehicle.getIDList():
                veh_logger.on_step_collect(vid)

            for vid in traci.simulation.getArrivedIDList():
                veh_logger.on_arrive(vid, t)

            stop_logger.step(t)

            control_step(args, t, veh_logger, stop_logger)

            if int(t)//60 != last_print_min:
                last_print_min = int(t)//60
                print(f"[sim] t={int(t)}s, remaining={traci.simulation.getMinExpectedNumber()}")

    finally:
        t_final = traci.simulation.getTime()
        traci.close(False)

    stop_rows = stop_logger.finalize(t_final)
    if not stop_rows:
        stop_rows = parse_stopinfo_xml(stopinfo_xml)

    # Write outputs
    write_csv(stop_rows, f_stop, ["veh", "stop", "arrival_s", "departure_s", "dwell_s"])
    write_csv(veh_logger.rows, f_per, ["veh", "class", "is_cav", "depart_s", "arrive_s", "travel_s", "delay_s"])
    write_run_summary(veh_logger.rows, f_sum)

    print("Done.")

    try:
        generate_plots(args)
    except Exception as e:
        print("Plotting error:", e)



def main():
    args = parse_args()
    run(args)
def generate_plots(args):
    import os, re
    import numpy as np
    import pandas as pd
    import matplotlib as mpl
    import matplotlib.pyplot as plt
    from matplotlib.ticker import MultipleLocator

    mpl.rcParams.update({
        "font.size": 8,
        "axes.labelsize": 6,
        "xtick.labelsize": 6,
        "ytick.labelsize": 6,
        "legend.fontsize": 9,
        "axes.linewidth": 0.6,
    })

    FIGSIZE = (6, 4)
    DPI = 150
    LINEWIDTH = 1.4
    FONT_LABEL = 14
    FONT_TICK = 12
    LEGEND_LOC = "upper left"
    LEGEND_FONTSIZE = 10
    LEGEND_FRAME = False
    GRID_DT = 1.0
    XTICK_STEP = 600.0

    base_dir = os.path.dirname(os.path.abspath(args.outdir))
    scen_5_7 = {
        "Case 1": os.path.join(base_dir, "srp_no_DL"),
        "Case 2": os.path.join(base_dir, "srp_DL"),
        "Case 3": os.path.join(base_dir, "drp"),
        "Case 4": os.path.join(base_dir, "proposed"),
    }
    scen_11 = [
        ("results_case1", "SRP without using joint DL"),
        ("results_case2", "SRP using joint DL"),
        ("results_case3", "Proposed method"),
    ]

    def _load_per(folder):
        p = os.path.join(folder, "per_vehicle.csv")
        if not os.path.isfile(p):
            return None
        df = pd.read_csv(p)
        if "class" in df.columns:
            df["class"] = df["class"].astype(str).str.strip().str.lower()
        return df

    def _time_window(dfs):
        vals = []
        for df in dfs.values():
            if df is not None and "arrive_s" in df.columns:
                vals.append(df["arrive_s"].values)
        if not vals:
            return 0.0, 0.0
        allv = np.concatenate(vals)
        return float(np.nanmin(allv)), float(np.nanmax(allv))

    def _step_at(times, values, t_query):
        idx = np.searchsorted(times, t_query, side="right") - 1
        return 0.0 if idx < 0 else values[idx]

    def _to_grid(times, values, t0, t1, dt):
        g = np.arange(t0, t1 + 1e-9, dt)
        v = np.array([_step_at(times, values, x) for x in g])
        return g, v

    def _cum_series(df_sub, value_col, t0, t1):
        d = df_sub.sort_values("arrive_s").copy()
        t = [t0]
        y = [0.0]
        acc = 0.0
        for tt, val_s in zip(d["arrive_s"].values, d[value_col].values):
            if t0 <= tt <= t1:
                acc += val_s / 60.0
                t.append(tt)
                y.append(acc)
        if t[-1] < t1:
            t.append(t1)
            y.append(acc)
        return np.array(t), np.array(y)

    def _apply_ticks(ax, xstep, ystep, xmin, xmax):
        if xstep and xstep > 0:
            ax.set_xticks(np.arange(xmin, xmax + 1e-9, xstep))
        if ystep and ystep > 0:
            lo, hi = ax.get_ylim()
            ax.set_yticks(np.arange(max(0.0, lo), hi + 1e-9, ystep))

    dfs_5_7 = {k: _load_per(v) for k, v in scen_5_7.items()}
    if any(df is not None for df in dfs_5_7.values()):
        t0, t1 = _time_window(dfs_5_7)
        xmin, xmax = 0.0, max(0.0, t1 - t0)
        series_bus = {}
        for cname, df in dfs_5_7.items():
            if df is None:
                continue
            need = {"veh", "class", "arrive_s", "travel_s"}
            if not need.issubset(df.columns):
                continue
            sub = df[(df["class"] == "bus") & (df["arrive_s"] >= t0) & (df["arrive_s"] <= t1)].copy()
            if len(sub) == 0:
                continue
            t, y = _cum_series(sub, "travel_s", t0, t1)
            tg, yg = _to_grid(t, y, t0, t1, GRID_DT)
            series_bus[cname] = (tg, yg)

        if "Case 1" in series_bus:
            tg = series_bus["Case 1"][0]
            y1 = series_bus["Case 1"][1]
            color_map = {"Case 2": "tab:orange", "Case 4": "tab:blue", "Case 3": "tab:green"}
            label_map = {"Case 2": "SRP without rerouting", "Case 4": "DRP with rerouting", "Case 3": "Proposed method"}
            fig, ax = plt.subplots(figsize=FIGSIZE, dpi=DPI)
            for cname in ["Case 2", "Case 4", "Case 3"]:
                if cname not in series_bus:
                    continue
                y = np.maximum(0.0, series_bus[cname][1] - y1)
                ax.plot(tg - t0, y, color=color_map.get(cname, None), linewidth=LINEWIDTH, label=label_map.get(cname, cname))
            ax.set_xlim(xmin, xmax)
            lo, hi = ax.get_ylim()
            ax.set_ylim(0, hi)
            ax.set_xlabel("Time [s]", fontsize=FONT_LABEL)
            ax.set_ylabel("Cumulative delay [min]", fontsize=FONT_LABEL)
            ax.tick_params(axis="both", labelsize=FONT_TICK)
            _apply_ticks(ax, XTICK_STEP, 2.0, xmin, xmax)
            ax.legend(loc=LEGEND_LOC, fontsize=LEGEND_FONTSIZE, frameon=LEGEND_FRAME)
            plt.tight_layout()
            plt.savefig(os.path.join(args.outdir, "fig_5_bus_delta.png"), dpi=DPI)
            plt.close()

        for cls, ylab in [("cav", "Total travel time [min]"), ("reg", "Total travel time [min]")]:
            per_case = {}
            for cname, df in dfs_5_7.items():
                if df is None:
                    continue
                need = {"veh", "class", "arrive_s", "travel_s"}
                if not need.issubset(df.columns):
                    continue
                sub = df[(df["class"] == cls) & (df["arrive_s"] >= t0) & (df["arrive_s"] <= t1)].sort_values("arrive_s")
                if len(sub) > 0:
                    per_case[cname] = sub
            if not per_case:
                continue
            n = min(len(x) for x in per_case.values())
            for c in list(per_case.keys()):
                if len(per_case[c]) > n:
                    idx = np.linspace(0, len(per_case[c]) - 1, n, dtype=int)
                    per_case[c] = per_case[c].iloc[idx, :].copy()
            color_map = {"Case 2": "tab:orange", "Case 4": "tab:blue", "Case 3": "tab:green"}
            label_map = {"Case 2": "SRP without rerouting", "Case 4": "DRP with rerouting", "Case 3": "Proposed method"}
            fig, ax = plt.subplots(figsize=FIGSIZE, dpi=DPI)
            for cname in ["Case 2", "Case 4", "Case 3"]:
                if cname not in per_case:
                    continue
                t, y = _cum_series(per_case[cname], "travel_s", t0, t1)
                tg, yg = _to_grid(t, y, t0, t1, GRID_DT)
                ax.plot(tg - t0, yg, color=color_map.get(cname, None), linewidth=LINEWIDTH, label=label_map.get(cname, cname))
            ax.set_xlim(xmin, xmax)
            lo, hi = ax.get_ylim()
            ax.set_ylim(0, hi)
            ax.set_xlabel("Time [s]", fontsize=FONT_LABEL)
            ax.set_ylabel(ylab, fontsize=FONT_LABEL)
            ax.tick_params(axis="both", labelsize=FONT_TICK)
            _apply_ticks(ax, XTICK_STEP, None, xmin, xmax)
            ax.legend(loc=LEGEND_LOC, fontsize=LEGEND_FONTSIZE, frameon=LEGEND_FRAME)
            plt.tight_layout()
            fname = "fig_6_cav_tt.png" if cls == "cav" else "fig_7_reg_tt.png"
            plt.savefig(os.path.join(args.outdir, fname), dpi=DPI)
            plt.close()

    UNITS = "min"
    VEHICLE_TYPES = ["Bus", "CAV", "HV"]
    BAR_WIDTH = 0.28
    GROUP_SPACING = 0.22
    AXIS_LABEL_SIZE = 14
    X_TICK_LABEL_SIZE = 12
    Y_TICK_LABEL_SIZE = 12
    LEGEND_FONT_SIZE = 13
    VALUE_FONT_SIZE = 11
    SHOW_VALUES = True
    VALUE_FORMAT = "{:.1f}"
    Y_TICK_INT = None
    Y_RANGE = (0, None)
    COLOR_BY_SCENARIO = {
        "SRP without using joint DL": "tab:gray",
        "SRP using joint DL": "tab:orange",
        "Proposed method": "tab:green",
    }

    def _to_units(vals):
        vals = np.asarray(vals, dtype=float)
        if UNITS.lower().startswith("min"):
            return vals / 60.0, "min"
        return vals, "s"

    def _annotate(ax, rects, values):
        for r, v in zip(rects, values):
            if SHOW_VALUES and np.isfinite(v):
                ax.text(r.get_x() + r.get_width() / 2, r.get_height(), VALUE_FORMAT.format(v), ha="center", va="bottom", fontsize=VALUE_FONT_SIZE)

    def _scenario_p90(folder_path):
        path = os.path.join(folder_path, "run_summary.csv")
        if not os.path.isfile(path):
            return None
        df = pd.read_csv(path)
        df["class"] = df["class"].astype(str).str.strip().str.lower()
        bus_val = df.loc[df["class"] == "bus", "p90_delay_s"].mean()
        cav_val = df.loc[df["class"] == "cav", "p90_delay_s"].mean()
        hv_val = df.loc[df["class"] == "reg", "p90_delay_s"].mean()
        return {"Bus": bus_val, "CAV": cav_val, "HV": hv_val}

    data_seconds = []
    scenario_labels = []
    for folder_name, nice_label in scen_11:
        folder = os.path.join(base_dir, folder_name)
        if not os.path.isdir(folder):
            continue
        m = _scenario_p90(folder)
        if m is None:
            continue
        data_seconds.append([m[v] for v in VEHICLE_TYPES])
        scenario_labels.append(nice_label)

    if scenario_labels:
        data_seconds = np.array(data_seconds, dtype=float)
        data_units, unit_label = _to_units(data_seconds)
        n_groups = len(VEHICLE_TYPES)
        n_scen = len(scenario_labels)
        group_width = n_scen * BAR_WIDTH + GROUP_SPACING
        x = np.arange(n_groups) * group_width
        plt.figure(figsize=(7, 5), dpi=DPI)
        for i, scen_label in enumerate(scenario_labels):
            vals = data_units[i, :]
            offsets = (i - (n_scen - 1) / 2) * BAR_WIDTH
            bars = plt.bar(x + offsets, vals, width=BAR_WIDTH, label=scen_label, color=COLOR_BY_SCENARIO.get(scen_label, None))
            _annotate(plt.gca(), bars, vals)
        plt.xlabel("", fontsize=AXIS_LABEL_SIZE)
        plt.ylabel("90th-percentile travel time [" + unit_label + "]", fontsize=AXIS_LABEL_SIZE)
        plt.xticks(x, VEHICLE_TYPES, fontsize=X_TICK_LABEL_SIZE)
        plt.tick_params(axis="y", labelsize=Y_TICK_LABEL_SIZE)
        if Y_RANGE[1] is not None:
            plt.ylim(Y_RANGE[0], Y_RANGE[1])
        plt.legend(fontsize=LEGEND_FONT_SIZE, loc="upper left", frameon=False)
        plt.tight_layout()
        plt.savefig(os.path.join(args.outdir, "fig_11_p90.png"), dpi=DPI)
        plt.close()
    
    
 

if __name__ == "__main__":
    main()


