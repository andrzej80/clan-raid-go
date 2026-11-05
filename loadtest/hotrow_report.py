#!/usr/bin/env python3
"""Отчёт эксперимента «горячая строка»: читает каталоги прогонов loadtest/hotrow.sh и строит
PNG-графики и markdown-таблицу.

    pip install matplotlib
    python loadtest/hotrow_report.py [loadtest/results/hotrow] [docs/hotrow]

Графики сохраняются картинками, чтобы результат можно было посмотреть без Grafana и стенда.
"""
import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path

import matplotlib
import matplotlib.ticker

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

MODES = ["kafka", "sync", "sync-full"]
COLORS = {"kafka": "#2a7de1", "sync": "#e8590c", "sync-full": "#9c36b5"}
SHAPE_TITLE = {
    "hot": "горячая строка (1 клан)",
    "spread": "нагрузка по многим кланам",
}


def read_pool(path: Path) -> dict[str, list[float]]:
    out: dict[str, list[float]] = {}
    if not path.exists():
        return out
    for line in path.read_text(encoding="utf-8").splitlines():
        if "\t" not in line:
            continue
        name, raw = line.split("\t", 1)
        try:
            result = json.loads(raw)["data"]["result"]
        except (ValueError, KeyError):
            continue
        values: list[float] = []
        for series in result:
            values += [float(v) for _, v in series["values"] if v not in ("NaN", "+Inf", "-Inf")]
        out[name] = values
    return out


def steady(rows: list, share: float = 0.6) -> list:
    """Последние share значений ряда: без разгона."""
    if not rows:
        return rows
    return rows[int(len(rows) * (1 - share)):]


def read_pg(path: Path) -> dict[str, float]:
    rows = []
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines()[1:]:
            parts = line.strip().split(",")
            if len(parts) == 6 and all(p.lstrip("-").isdigit() for p in parts):
                rows.append([int(p) for p in parts])
    rows = steady(rows)
    if not rows:
        return {"lock_wait_avg": 0, "lock_wait_max": 0, "active_avg": 0, "idle_in_tx_avg": 0, "conns_max": 0}
    col = lambda i: [r[i] for r in rows]  # noqa: E731
    return {
        "active_avg": statistics.mean(col(1)),
        "lock_wait_avg": statistics.mean(col(2)),
        "lock_wait_max": max(col(2)),
        "idle_in_tx_avg": statistics.mean(col(3)),
        "conns_max": max(col(5)),
    }


def load_case(d: Path) -> dict | None:
    try:
        meta = json.loads((d / "meta.json").read_text(encoding="utf-8"))
        rep = json.loads((d / "botsim.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    lat = rep.get("latency_ms", {})
    total = rep.get("rpc_total", 0)
    ok = rep.get("ok_responses", 0)
    lag_file = d / "lag.max"
    case = dict(meta)
    case.update(
        rps=rep.get("rps", 0.0),
        p50=lat.get("p50", 0.0),
        p95=lat.get("p95", 0.0),
        p99=lat.get("p99", 0.0),
        total=total,
        errors=total - ok,
        ok_pct=100 * ok / total if total else 0.0,
        ok_rps=ok / rep.get("elapsed_sec", 1) if rep.get("elapsed_sec") else 0.0,
        dropped=rep.get("dropped", 0),
        lag=int(lag_file.read_text().strip()) if lag_file.exists() else None,
    )
    case.update(read_pg(d / "pg.csv"))
    # Доля соединений пула api, которые стоят в ожидании блокировки строки. Метрики пула из
    # VictoriaMetrics (pool.tsv) сохраняются для разбора, но в отчёт не идут: при перегрузке api
    # экспорт метрик терял точки, и ряды получались неполными.
    case["lock_share"] = case["lock_wait_avg"] / meta["pool"] if meta["pool"] else 0.0
    return case


def style(ax, title, xlabel, ylabel, logy=False):
    ax.set_title(title, fontsize=10)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.grid(True, alpha=0.3)
    if logy:
        ax.set_yscale("log")


def plot_ladder(cases, key, ylabel, fname, outdir, logy=False, ideal=False, title=""):
    shapes = [s for s in ("hot", "spread") if any(c["shape"] == s for c in cases)]
    fig, axes = plt.subplots(1, len(shapes), figsize=(6 * len(shapes), 4.2), squeeze=False, sharey=True)
    for ax, shape in zip(axes[0], shapes):
        for mode in MODES:
            pts = sorted((c["target_rps"], c[key]) for c in cases if c["shape"] == shape and c["mode"] == mode and c[key] is not None)
            if pts:
                ax.plot(*zip(*pts), marker="o", label=mode, color=COLORS[mode])
        if ideal:
            xs = sorted({c["target_rps"] for c in cases if c["shape"] == shape})
            ax.plot(xs, xs, "--", color="gray", label="идеал (RPS = цель)")
        style(ax, SHAPE_TITLE[shape], "целевой RPS", ylabel, logy)
        ax.legend()
    fig.suptitle(title, fontsize=11)
    fig.tight_layout()
    fig.savefig(outdir / fname, dpi=130)
    plt.close(fig)


def plot_locks(cases, outdir):
    """Сколько backend'ов в среднем ждут блокировку (wait_event_type = Lock) на установившейся нагрузке."""
    plot_ladder([c for c in cases if c["mode"] != "kafka"], "lock_wait_avg", "ждут блокировку, соединений",
                "locks.png", outdir, title="Ожидание блокировок строки в Postgres (pg_stat_activity)")


def plot_pool_sweep(cases, outdir):
    sweep = [c for c in cases if c["block"] == "poolsweep"]
    if not sweep:
        return
    shapes = [s for s in ("hot", "spread") if any(c["shape"] == s for c in sweep)]
    fig, axes = plt.subplots(2, len(shapes), figsize=(6 * len(shapes), 7.5), squeeze=False)
    for col, shape in enumerate(shapes):
        for mode in ("sync", "sync-full"):
            pts = sorted((c["pool"], c) for c in sweep if c["shape"] == shape and c["mode"] == mode)
            if not pts:
                continue
            x = [p for p, _ in pts]
            axes[0][col].plot(x, [c["p99"] for _, c in pts], marker="o", label=mode, color=COLORS[mode])
            axes[1][col].plot(x, [100 * c["lock_share"] for _, c in pts], marker="o", label=mode, color=COLORS[mode])
        style(axes[0][col], SHAPE_TITLE[shape], "PG_MAX_CONNS у api", "p99, мс", logy=True)
        style(axes[1][col], "", "PG_MAX_CONNS у api", "соединений пула в ожидании блокировки, %")
        for ax in (axes[0][col], axes[1][col]):
            ax.set_xscale("log", base=2)
            ax.set_xticks(sorted({c["pool"] for c in sweep}))
            ax.get_xaxis().set_major_formatter(matplotlib.ticker.ScalarFormatter())
        axes[0][col].legend()
        axes[1][col].legend()
    rps = sweep[0]["target_rps"]
    fig.suptitle(f"Размер пула при {rps:.0f} RPS: помогает ли больше соединений", fontsize=11)
    fig.tight_layout()
    fig.savefig(outdir / "pool_sweep.png", dpi=130)
    plt.close(fig)


def table(cases) -> str:
    head = ("| блок | режим | форма | пул | цель RPS | успешных RPS | p50, мс | p95, мс | p99, мс | ошибки, % | "
            "ждут lock (ср./макс.) | лаг консьюмера |\n"
            "|---|---|---|---|---|---|---|---|---|---|---|---|\n")
    rows = []
    for c in sorted(cases, key=lambda c: (c["block"], c["shape"], MODES.index(c["mode"]), c["pool"], c["target_rps"])):
        lag = "-" if c["lag"] is None else str(c["lag"])
        err = 100 * c["errors"] / c["total"] if c["total"] else 0.0
        rows.append(
            f'| {c["block"]} | {c["mode"]} | {c["shape"]} | {c["pool"]} | {c["target_rps"]:.0f} | {c["ok_rps"]:.0f} | '
            f'{c["p50"]:.1f} | {c["p95"]:.1f} | {c["p99"]:.1f} | {err:.1f} | '
            f'{c["lock_wait_avg"]:.1f}/{c["lock_wait_max"]:.0f} | {lag} |'
        )
    return head + "\n".join(rows) + "\n"


def main() -> int:
    src = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("loadtest/results/hotrow")
    out = Path(sys.argv[2]) if len(sys.argv) > 2 else Path("docs/hotrow")
    cases = [c for d in sorted(src.iterdir()) if d.is_dir() and (c := load_case(d))]
    if not cases:
        print(f"no finished cases in {src}", file=sys.stderr)
        return 1
    out.mkdir(parents=True, exist_ok=True)
    ladder = [c for c in cases if c["block"] in ("ladder", "fine")]

    plot_ladder(ladder, "p99", "p99, мс", "p99.png", out, logy=True,
                title="Задержка p99 по режимам (потолок ~2005 мс: дедлайн клиента)")
    plot_ladder(ladder, "ok_pct", "успешных ответов, %", "success.png", out,
                title="Доля успешных ответов (остальное: DeadlineExceeded и повторы)")
    plot_ladder(ladder, "ok_rps", "успешных ответов в секунду", "throughput.png", out, ideal=True,
                title="Пропускная способность: успешные ответы в секунду против целевого RPS")
    plot_ladder(ladder, "p50", "p50, мс", "p50.png", out, logy=True, title="Медианная задержка по режимам")
    plot_locks(ladder, out)
    plot_pool_sweep(cases, out)

    (out / "results.md").write_text(table(cases), encoding="utf-8")
    print(f"{len(cases)} cases -> {out}/ (p99.png, throughput.png, p50.png, success.png, locks.png, pool_sweep.png, results.md)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
