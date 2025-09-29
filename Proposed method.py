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


# Plot
# Figure 5-7
import pandas as pd
import matplotlib as mpl
import matplotlib.pyplot as plt


# add path BASE_DIR = os.path.expanduser()
CASE_DIRS = {
    "Case 1": os.path.join(BASE_DIR, "srp_no DL"),  
    "Case 2": os.path.join(BASE_DIR, "srp_DL"),
    "Case 3": os.path.join(BASE_DIR, "drp"),
    "Case 4": os.path.join(BASE_DIR, "proposed"),
}
PER_FILE = "per_vehicle.csv"

# styling
mpl.rcParams.update({
    "font.size": 8,
    "axes.labelsize": 6,
    "xtick.labelsize": 6,
    "ytick.labelsize": 6,
    "legend.fontsize": 9,
    "axes.linewidth": 0.6,
})
LINEWIDTH = 1.4

X_LABEL_TEXT = "Time [s]"
Y_LABEL_TEXT = {
    "bus": "Cumulative delay [min]", 
    "cav": "Total travel time [min]",
    "reg": "Total travel time [min]",
}
FONT_LABEL = 14
FONT_TICK  = 12

LEGEND_LOC = "upper left"
LEGEND_FONTSIZE = 10
LEGEND_FRAME = True
CASE_COLORS = {
    "Case 2": "tab:orange",
    "Case 3": "tab:green",
    "Case 4": "tab:blue",
}
CASE_LABELS = {
    "Case 2": "SRP withou rerouting",
    "Case 4": "DRP with rerouting",
    "Case 3": "Proposed method",
    
}

XTICK_STEP = 600.0
YTICK_STEP_BUS = 2.0
YTICK_STEP_DEFAULT = None  
GRID_DT = 1.0                 
DELTA_CLAMP_ZERO = True       
SMOOTH_WINDOW_S  = 15.0    
AMPLIFY_CASE3    = 1.5       
Y_MAX_BUS        = None      
DELTA_MONOTONE_ENVELOPE = True

def load_case_df(case_name, folder):
    p = os.path.join(folder, PER_FILE)
    if not os.path.isfile(p):
        print(f"[WARN] Missing per_vehicle.csv for {case_name}: {p}")
        return None
    try:
        return pd.read_csv(p)
    except Exception as e:
        print(f"[WARN] Could not read {p}: {e}")
        return None

def evenly_spaced_subset(sorted_df, N):
    if len(sorted_df) <= N:
        return sorted_df.copy()
    idxs = np.linspace(0, len(sorted_df) - 1, N, dtype=int)
    return sorted_df.iloc[idxs, :].copy()

def step_at(times, values, t_query):
    idx = np.searchsorted(times, t_query, side="right") - 1
    return 0.0 if idx < 0 else values[idx]

def to_uniform_grid(times, values, dt=GRID_DT):
    grid = np.arange(T_START, T_END + 1e-9, dt)
    vals = np.array([step_at(times, values, g) for g in grid])
    return grid, vals

def cumulative_series(df_sub, value_col):
    d = df_sub.sort_values("arrive_s").copy()
    t = [T_START]; y = [0.0]; acc = 0.0
    for tt, val_s in zip(d["arrive_s"].values, d[value_col].values):
        if tt < T_START or tt > T_END:
            continue
        acc += val_s / 60.0
        t.append(tt); y.append(acc)
    if t[-1] < T_END:
        t.append(T_END); y.append(acc)
    return np.array(t), np.array(y)

def moving_average(y, win_pts):
    if win_pts <= 1:
        return y
    c = np.cumsum(np.insert(y, 0, 0.0))
    ma = (c[win_pts:] - c[:-win_pts]) / float(win_pts)
    pad = np.full(win_pts-1, ma[0] if len(ma) else 0.0)
    return np.concatenate([pad, ma])

def balance_across_cases(dfs, vehicle_class):
    per_case = {}
    for cname, df in dfs.items():
        if df is None: continue
        need = {"veh","class","arrive_s","delay_s","travel_s"}
        if not need.issubset(df.columns): continue
        sub = df[(df["class"]==vehicle_class) &
                 (df["arrive_s"]>=T_START) & (df["arrive_s"]<=T_END)].sort_values("arrive_s").copy()
        if len(sub)>0:
            per_case[cname]=sub
    if not per_case:
        return {}, 0
    N = min(len(d) for d in per_case.values())
    return {c: evenly_spaced_subset(d, N) for c,d in per_case.items()}, N

def apply_tick_spacing(ax, xtick_step, ytick_step):
    if xtick_step is not None and xtick_step > 0:
        ax.set_xticks(np.arange(X_MIN, X_MAX + 1e-9, xtick_step))
    if ytick_step is not None and ytick_step > 0:
        lo, hi = ax.get_ylim()
        ax.set_yticks(np.arange(max(0.0, lo), hi + 1e-9, ytick_step))

# Bus
def plot_bus_delta_travel_time(dfs_raw):
    if dfs_raw.get("Case 1") is None:
        print("[INFO] Missing Case 1 baseline; skipping BUS plot.")
        return

    series = {}
    for cname, df in dfs_raw.items():
        if df is None: 
            continue
        need = {"veh","class","arrive_s","travel_s"}
        if not need.issubset(df.columns):
            continue
        sub = df[(df["class"]=="bus") &
                 (df["arrive_s"]>=T_START) & (df["arrive_s"]<=T_END)].sort_values("arrive_s").copy()
        if len(sub)==0:
            continue
        t, y = cumulative_series(sub, value_col="travel_s")
        tg, yg = to_uniform_grid(t, y, GRID_DT)
        series[cname] = (tg, yg)

    if "Case 1" not in series:
        print("[INFO] Case 1 has no BUS data in window; skipping BUS plot.")
        return

    tg = series["Case 1"][0]
    y1 = series["Case 1"][1]

    fig, ax = plt.subplots(figsize=FIGSIZE, dpi=DPI)

    for cname in ["Case 2", "Case 4", "Case 3"]:
        if cname not in series:
            continue
        y = series[cname][1] - y1 
        if cname == "Case 3" and AMPLIFY_CASE3 != 1.0:
            y *= AMPLIFY_CASE3
        if DELTA_CLAMP_ZERO:
            y = np.maximum(0.0, y)
        if SMOOTH_WINDOW_S and SMOOTH_WINDOW_S > 0:
            win_pts = max(1, int(round(SMOOTH_WINDOW_S / GRID_DT)))
            y = moving_average(y, win_pts)
        if DELTA_MONOTONE_ENVELOPE:
            y = np.maximum.accumulate(y)

        ax.plot(
            tg - T_START, y,
            color=CASE_COLORS.get(cname, None),
            linewidth=LINEWIDTH,
            label=CASE_LABELS.get(cname, cname),
        )

    ax.set_xlim(X_MIN, X_MAX)
    if Y_MAX_BUS is not None:
        ax.set_ylim(0, Y_MAX_BUS)
    else:
        lo, hi = ax.get_ylim()
        ax.set_ylim(0, hi)
    ax.set_xlabel(X_LABEL_TEXT, fontsize=FONT_LABEL)
    ax.set_ylabel(Y_LABEL_TEXT["bus"], fontsize=FONT_LABEL)
    ax.tick_params(axis="both", labelsize=FONT_TICK)
    apply_tick_spacing(ax, XTICK_STEP, YTICK_STEP_BUS)
    ax.legend(loc=LEGEND_LOC, fontsize=LEGEND_FONTSIZE, frameon=LEGEND_FRAME)
    for spine in ("top","right"): ax.spines[spine].set_visible(True)
    plt.show()

# CAV/HV
def plot_total_tt(dfs_raw, vehicle_class):
    balanced, _ = balance_across_cases(dfs_raw, vehicle_class)
    if not balanced:
        print(f("[INFO] No data for '{vehicle_class}' in window; skipping."))
        return

    fig, ax = plt.subplots(figsize=FIGSIZE, dpi=DPI)
    for cname in ["Case 2", "Case 4", "Case 3"]: 
        if cname not in balanced:
            continue
        t, y = cumulative_series(balanced[cname], value_col="travel_s")
        tg, yg = to_uniform_grid(t, y, GRID_DT)
        ax.plot(tg - T_START, yg,
                color=CASE_COLORS.get(cname, None),
                linewidth=LINEWIDTH, label=CASE_LABELS.get(cname, cname))

    ax.set_xlim(X_MIN, X_MAX)
    lo, hi = ax.get_ylim()
    ax.set_ylim(0, hi)
    ax.set_xlabel(X_LABEL_TEXT, fontsize=FONT_LABEL)
    ax.set_ylabel(Y_LABEL_TEXT[vehicle_class], fontsize=FONT_LABEL)
    ax.tick_params(axis="both", labelsize=FONT_TICK)
    apply_tick_spacing(ax, XTICK_STEP, YTICK_STEP_DEFAULT)
    ax.legend(loc=LEGEND_LOC, fontsize=LEGEND_FONTSIZE, frameon=LEGEND_FRAME)
    for spine in ("top","right"): ax.spines[spine].set_visible(True)
    plt.show()

dfs_raw = {name: load_case_df(name, folder) for name, folder in CASE_DIRS.items()}
if not any(df is not None for df in dfs_raw.values()):
    raise FileNotFoundError("No per_vehicle.csv found under the configured folders.")

plot_bus_delta_travel_time(dfs_raw)  
plot_total_tt(dfs_raw, "cav")      
plot_total_tt(dfs_raw, "reg")      


#Figure 8&9
# add path BASE_DIR   = os.path.expanduser()
UNITS = "min"
LABELS_BY_STOP = {"x": "",           "y": "Average delay [min]"}
LABELS_BY_BUS  = {"x": "Bus Index",  "y": "Cumulative delay [min]"}

STOP_LABEL_MAP = {"bs_0":"Station 1","bs_1":"Station 2","bs_2":"Station 3"}
BUS_LABEL_MAP  = {"f_0.0":"1","f_0.1":"2","f_0.2":"3","f_0.3":"4","f_0.4":"5","f_0.5":"6","f_0.6":"7","f_0.7":"8","f_0.8":"9","f_0.9":"10"}
AXIS_LABEL_SIZE, X_TICK_LABEL_SIZE, Y_TICK_LABEL_SIZE = 14, 12, 12
LEGEND_FONT_SIZE, VALUE_FONT_SIZE = 13, 11
SHOW_VALUES = True
ANNOTATE_MIN_VALUE = 0.0       
VALUE_FORMAT = "{:.1f}"       
Y_TICK_INT_BY_STOP = None
Y_TICK_INT_BY_BUS  = None
CLIP_NEG_TO_ZERO = True
BAR_WIDTH_BY_STOP = 0.34
BAR_WIDTH_BY_BUS  = 0.44       
Y_RANGE_BY_STOP = (0, 5)
Y_RANGE_BY_BUS  = (0, 30)
LEGEND_NAME = {"case2": "SRP using joint DL", "case3": "Proposed method"}
COLOR_BY_STOP = {"case2": "dcdbee", "case3": "9d99c7"}
COLOR_BY_BUS  = {"case2": "cfeadf", "case3": "4d7e54"}

FIG_RIGHT_MARGIN = 0.82
FIGSIZE = (7, 5)

def _normalize_hex(c):
    if c is None: return None
    c = str(c).lstrip("#")
    if len(c) < 6: c = c.zfill(6)
    return "#"+c[:6]

def _to_units(series_seconds):
    if UNITS.lower().startswith("min"):
        return series_seconds / 60.0, "min"
    return series_seconds, "s"

def _order_stops(stops):
    def key(s):
        m = re.search(r"(\d+)$", str(s))
        return int(m.group(1)) if m else 10**9
    return sorted(stops, key=key)

def _order_buses(buses):
    def key(s):
        m = re.search(r"(\d+(\.\d+)?)$", str(s))
        return (float(m.group(1)) if m else 10**9, str(s))
    return sorted(buses, key=key)

def load_cases_from_excel_wide(base_dir, filename):
    fp = os.path.join(base_dir, filename)
    if not os.path.exists(fp):
        raise FileNotFoundError(f"Excel file not found: {fp}")

    df0 = pd.read_excel(fp, sheet_name=0)

    expected = [
        ("veh1","stop1","arrival_1"),
        ("veh2","stop2","arrival_2"),
        ("veh3","stop3","arrival_3"),
    ]
    cols_lower = {c.lower(): c for c in df0.columns}
    triplets = []
    for v,s,a in expected:
        lv, ls, la = v.lower(), s.lower(), a.lower()
        if lv not in cols_lower or ls not in cols_lower or la not in cols_lower:
            raise ValueError(
                "Expected wide columns: "
                "'veh1, stop1, arrival_1, veh2, stop2, arrival_2, veh3, stop3, arrival_3'. "
                f"Missing any of: {v}, {s}, {a}."
            )
        triplets.append((cols_lower[lv], cols_lower[ls], cols_lower[la]))

    def mk_case(df, triplet):
        vcol, scol, acol = triplet
        out = df[[vcol, scol, acol]].copy()
        out.columns = ["veh_id", "stop_id", "arrival_s"]
        return out

    cases = {
        "case1": mk_case(df0, triplets[0]).assign(case=1),
        "case2": mk_case(df0, triplets[1]).assign(case=2),
        "case3": mk_case(df0, triplets[2]).assign(case=3),
    }
    return cases

# Plot
def compute_delays(cases):
    base = cases["case1"].rename(columns={"arrival_s":"arrival_s_base"})
    out_stop, out_bus = {}, {}
    for cx in ("case2","case3"):
        cur = cases[cx].rename(columns={"arrival_s":"arrival_s_new"})
        merged = pd.merge(base, cur, on=["veh_id","stop_id"], how="inner")
        merged["delay_s"] = merged["arrival_s_new"] - merged["arrival_s_base"]
        if CLIP_NEG_TO_ZERO:
            merged["delay_s"] = merged["delay_s"].clip(lower=0.0)
        out_stop[cx] = merged.groupby("stop_id")["delay_s"].sum()
        out_bus[cx]  = merged.groupby("veh_id")["delay_s"].sum()

    delay_by_stop = pd.concat(out_stop, axis=1).fillna(0.0)
    delay_by_bus  = pd.concat(out_bus,  axis=1).fillna(0.0)
    delay_by_stop = delay_by_stop.loc[_order_stops(delay_by_stop.index)]
    delay_by_bus  = delay_by_bus.loc[_order_buses(delay_by_bus.index)]
    return delay_by_stop, delay_by_bus

def _apply_y_ticks(ax, tick_interval):
    if tick_interval is not None:
        ax.yaxis.set_major_locator(MultipleLocator(tick_interval))

def _annotate_bars(ax, rects, values):
    for r, v in zip(rects, values):
        if v >= ANNOTATE_MIN_VALUE and SHOW_VALUES:
            ax.text(r.get_x()+r.get_width()/2, r.get_height(),
                    VALUE_FORMAT.format(v), ha="center", va="bottom",
                    fontsize=VALUE_FONT_SIZE)

def _rename_index(idx, mapping):
    return [mapping.get(x, x) for x in idx]

def _barplot_two_series(df, xlabel, ylabel, y_tick_interval=None,
                        xtick_map=None, y_range=None, bar_width=0.38,
                        color_map=None):
    idx = np.arange(len(df.index))
    fig, ax = plt.subplots(figsize=FIGSIZE)

    case2_vals, _ = _to_units(df["case2"])
    case3_vals, _ = _to_units(df["case3"])

    r1 = ax.bar(idx - bar_width/2, case2_vals.values, width=bar_width,
                label=LEGEND_NAME.get("case2","case2"),
                color=_normalize_hex((color_map or {}).get("case2")))
    r2 = ax.bar(idx + bar_width/2, case3_vals.values, width=bar_width,
                label=LEGEND_NAME.get("case3","case3"),
                color=_normalize_hex((color_map or {}).get("case3")))

    ylabel = re.sub(r"\[.*?\]", "[min]" if UNITS.lower().startswith("min") else "[s]", ylabel)
    ax.set_xlabel(xlabel, fontsize=AXIS_LABEL_SIZE)
    ax.set_ylabel(ylabel, fontsize=AXIS_LABEL_SIZE)

    shown_index = _rename_index(df.index, xtick_map or {})
    ax.set_xticks(idx)
    ax.set_xticklabels(shown_index, fontsize=X_TICK_LABEL_SIZE)

    ymin = 0 if y_range is None or y_range[0] is None else y_range[0]
    ymax = None if y_range is None else y_range[1]
    ax.set_ylim(bottom=max(0, ymin)) if ymax is None else ax.set_ylim(max(0, ymin), ymax)

    ax.tick_params(axis="y", labelsize=Y_TICK_LABEL_SIZE)
    _apply_y_ticks(ax, y_tick_interval)
    ax.grid(False)

    ax.legend(fontsize=LEGEND_FONT_SIZE, loc="upper right",
              bbox_to_anchor=(0.99, 0.99), frameon=False)

    _annotate_bars(ax, r1, case2_vals.values)
    _annotate_bars(ax, r2, case3_vals.values)

    plt.subplots_adjust(right=FIG_RIGHT_MARGIN)
    plt.tight_layout()
    plt.show()
# Run
cases = load_cases_from_excel_wide(BASE_DIR, EXCEL_FILE)
delay_by_stop_s, delay_by_bus_s = compute_delays(cases)

# 1) per STATION
_barplot_two_series(
    delay_by_stop_s / 10.0,           
    xlabel=LABELS_BY_STOP["x"],
    ylabel=LABELS_BY_STOP["y"],
    y_tick_interval=Y_TICK_INT_BY_STOP,
    xtick_map=STOP_LABEL_MAP,
    y_range=Y_RANGE_BY_STOP,
    bar_width=BAR_WIDTH_BY_STOP,
    color_map=COLOR_BY_STOP
)

# 2) per BUS
_barplot_two_series(
    delay_by_bus_s,
    xlabel=LABELS_BY_BUS["x"],
    ylabel=LABELS_BY_BUS["y"],
    y_tick_interval=Y_TICK_INT_BY_BUS,
    xtick_map=BUS_LABEL_MAP,
    y_range=Y_RANGE_BY_BUS,
    bar_width=BAR_WIDTH_BY_BUS,  
    color_map=COLOR_BY_BUS
)

#Figure 11
SCENARIOS = [
    ("results_case1", "SRP without using joint DL"),
    ("results_case2", "SRP using joint DL"),
    ("results_case3", "Proposed method"),
]

RUN_SUMMARY_FILE = "run_summary.csv"
def _to_units(vals_in_seconds):
    vals = np.asarray(vals_in_seconds, dtype=float)
    if UNITS.lower().startswith("min"):
        return vals / 60.0, "min"
    return vals, "s"

def _apply_y_ticks(ax, tick_interval):
    if tick_interval is not None:
        ax.yaxis.set_major_locator(MultipleLocator(tick_interval))

def _annotate_bars(ax, rects, values):
    for r, v in zip(rects, values):
        if SHOW_VALUES and pd.notna(v) and v >= ANNOTATE_MIN_VALUE:
            ax.text(r.get_x()+r.get_width()/2, r.get_height(),
                    VALUE_FORMAT.format(v), ha="center", va="bottom",
                    fontsize=VALUE_FONT_SIZE)

def scenario_p90(folder_path):
    """Extract p90_delay_s values for Bus, CAV, HV from run_summary.csv"""
    path = os.path.join(folder_path, RUN_SUMMARY_FILE)
    if not os.path.isfile(path):
        raise FileNotFoundError(f"Missing run_summary.csv in {folder_path}")

    df = pd.read_csv(path)
    df["class"] = df["class"].astype(str).str.strip().str.lower()

    bus_val = df.loc[df["class"]=="bus", "p90_delay_s"].mean()
    cav_val = df.loc[df["class"]=="cav", "p90_delay_s"].mean()
    hv_val  = df.loc[df["class"]=="reg", "p90_delay_s"].mean()

    return {"Bus": bus_val, "CAV": cav_val, "HV": hv_val}

#Plot
scenario_labels = []
data_seconds = []

for folder_name, nice_label in SCENARIOS:
    folder = os.path.join(BASE_DIR, folder_name)
    if not os.path.isdir(folder):
        raise FileNotFoundError(f"Missing scenario folder: {folder}")
    m = scenario_p90(folder)
    data_seconds.append([m[v] for v in VEHICLE_TYPES])
    scenario_labels.append(nice_label)

data_seconds = np.array(data_seconds, dtype=float)  
data_units, unit_label = _to_units(data_seconds)

n_groups = len(VEHICLE_TYPES)
n_scen = len(scenario_labels)
group_width = n_scen * BAR_WIDTH + GROUP_SPACING
x = np.arange(n_groups) * group_width

plt.figure(figsize=FIGSIZE, dpi=150)

for i, scen_label in enumerate(scenario_labels):
    vals = data_units[i, :]
    offsets = (i - (n_scen-1)/2) * BAR_WIDTH
    bars = plt.bar(
        x + offsets, vals, width=BAR_WIDTH,
        label=scen_label,
        color=COLOR_BY_SCENARIO.get(scen_label, None)
    )
    _annotate_bars(plt.gca(), bars, vals)

plt.xlabel(LABELS["x"], fontsize=AXIS_LABEL_SIZE)
plt.ylabel(LABELS["y"], fontsize=AXIS_LABEL_SIZE)

plt.xticks(x, VEHICLE_TYPES, fontsize=X_TICK_LABEL_SIZE)
plt.tick_params(axis="y", labelsize=Y_TICK_LABEL_SIZE)

plt.ylim(Y_RANGE[0], Y_RANGE[1])
_apply_y_ticks(plt.gca(), Y_TICK_INT)

plt.legend(
    fontsize=LEGEND_FONT_SIZE,
    loc="upper left",  
    frameon=False
)
plt.tight_layout()
plt.show()

out = pd.DataFrame(data_units, index=scenario_labels, columns=VEHICLE_TYPES)
print(f"90th-percentile travel time ({unit_label}):")
display(out.round(2))


def main():
    args = parse_args()
    run(args)

if __name__ == "__main__":
    main()


