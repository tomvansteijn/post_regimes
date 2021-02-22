#! /usr/bin/env python
# -*- coding: utf-8 -*-
# Tom van Steijn

from matplotlib import pyplot as plt
import pandas as pd
import requests

from pathlib import Path
import argparse
import calendar
import logging
import json
import yaml
import os

log = logging.getLogger(os.path.basename(__file__))


def get_parser():
    """get argumentparser and add arguments"""
    parser = argparse.ArgumentParser(
        "description",
    )

    # Command line arguments
    parser.add_argument(
        "inputfile", type=str, help=("YAML input file containing keyword arguments")
    )
    parser.add_argument(
        "credentialsfile",
        type=str,
        help=("YAML input file containing keyword arguments"),
    )
    return parser


def setup_filelogging(dirname="log", level=logging.DEBUG):
    # create directory
    logdir = Path(dirname)
    logdir.mkdir(exist_ok=True)

    # filehandler
    timestamp = pd.Timestamp.today().strftime("%Y%m%d%H%M%S")
    logfile = logdir / f"{timestamp:}.log"
    filehandler = logging.FileHandler(logfile)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    filehandler.setFormatter(formatter)

    # file logger
    filelog = logging.getLogger(os.path.basename(__file__))
    filelog.setLevel(level)
    filelog.addHandler(filehandler)

    return filelog


def get_mean_regime(series, stats):
    """Get mean regime in function of daynumber"""
    regime = series.groupby(series.index.dayofyear).aggregate(stats)
    regime.index.name = "daynumber"
    return regime


def plot_regime(pngfile, series, mean_regime, loc, years):
    # plot figure
    fig, ax = plt.subplots(figsize=(12.0, 6.0))
    bxa = []

    # define xticks using 2020 calendar
    xticks = [pd.Timestamp(2020, m + 1, 1).dayofyear for m in range(12)]

    # plot years
    for year in years:
        in_year = series.index.year == year
        if not in_year.any():
            continue
        year_regime = series.loc[in_year]
        year_regime.index = year_regime.index.dayofyear

        if year == years[-1]:
            linewidth = 2.0
        else:
            linewidth = 1.0

        ax.plot(
            year_regime.index,
            year_regime,
            linewidth=linewidth,
            label=f"{year:d}",
            zorder=3,
        )

    # plot mean regime
    ax.plot(
        mean_regime.index,
        mean_regime.loc[:, "regime_mean"],
        linestyle="--",
        color="darkgray",
        label="gemiddelde",
        zorder=2,
    )
    ax.fill_between(
        mean_regime.index,
        mean_regime.loc[:, "regime_min"],
        mean_regime.loc[:, "regime_max"],
        color="lightgray",
        alpha=0.5,
        label="min - max",
        zorder=1,
    )

    # formatting
    ax.grid()

    ax.set_xlim([1.0, 366.0])
    ax.set_xticks(xticks)

    ax.set_xticklabels(calendar.month_abbr[1:])

    ax.set_xlabel("maand")
    ax.set_ylabel("stijghoogte [m+NAP]")

    ttl = ax.set_title(f"{loc:}")
    bxa.append(ttl)

    lgd = ax.legend(loc="upper left", bbox_to_anchor=(1.0, 1.0))
    bxa.append(lgd)

    # save figure
    plt.savefig(
        pngfile,
        bbox_inches="tight",
        bbox_extra_artists=bxa,
        dpi=200.0,
    )
    plt.close()


def tz_naive(series):
    series.index = series.index.tz_localize(None)
    return series


def data_records(series):
    records = []
    for date, value in series.iteritems():
        records.append({"time": date.strftime("%Y-%m-%dT%H:%M:%SZ"), "value": value})
    return records


def get_timeseries(response_result):
    series = pd.DataFrame(response_result)
    series.loc[:, "first_timestamp"] = pd.to_datetime(series.loc[:, "first_timestamp"])
    series = (
        series.set_index("first_timestamp")
        .loc[:, "avg"]
        .sort_index()
        .pipe(lambda s: tz_naive(s))
        .resample("d")
        .mean()
    )
    series.name = "value"
    return series


def run(**kwargs):
    # unpack input from kwargs
    lizardapi = kwargs["lizardapi"]
    credentials = kwargs["credentials"]
    organisation = kwargs["organisation"]
    period_mean = kwargs["period_mean"]
    nlocs = kwargs.get("nlocs", 1)
    timeseries_fields = kwargs["timeseries_fields"]
    plot = kwargs.get("plot", False)
    post = kwargs.get("post", False)
    plot_years = kwargs.get("plot_years")

    # log to file
    flog = setup_filelogging()

    # today
    today = pd.Timestamp.today()

    # period for which mean regime is calculated
    start, end = pd.to_datetime(period_mean)

    # headers
    headers = {
        "Content-Type": "application/json",
        "username": credentials.get("username"),
        "password": credentials.get("password"),
    }

    # get locations
    loc_url = lizardapi + "/locations"
    loc_params = {
        "pagesize": f"{nlocs:d}",
        "organisation__uuid": organisation["uuid"],
    }
    get_loc_rs = requests.get(url=loc_url, headers=headers, params=loc_params).json()
    for loc in get_loc_rs["results"]:
        log.info(f"location {loc['name']:}")

        # get location timeseries
        ts_url = lizardapi + "/timeseries"
        ts_params = {
            "page_size": "1000000000",
            "name": "WNS9040",
            "location__uuid": loc["uuid"],
        }
        # flog.info(f"get timeseries for location {loc['name']:}")
        get_ts_rs = requests.get(url=ts_url, headers=headers, params=ts_params).json()        
        for ts in get_ts_rs["results"]:
            
            # skip if not in current year
            ts_end = pd.to_datetime(ts["end"])
            if ts_end.year < today.year:
                continue

            # get timeseries aggregate
            agg_url = lizardapi + "/timeseries/" + ts["uuid"] + "/aggregates"
            agg_params = {
                "page_size": "1000000000",
                "window": "day",
                "fields": "first_timestamp,avg",
                "start": start,
                "end": pd.Timestamp.today(),
            }
            get_agg_rs = requests.get(
                url=agg_url, headers=headers, params=agg_params
            ).json()
            series = get_timeseries(get_agg_rs["results"])

            # calculate mean regime
            stats = ["mean", "min", "max"]
            series_period = series.truncate(before=start, after=end)
            mean_regime = get_mean_regime(series_period, stats=stats)
            mean_regime.columns = [f"regime_{c:}" for c in mean_regime.columns]

            # join series and mean regime on day number
            frame = series.to_frame().set_index(series.index.dayofyear, append=True)
            frame.index.names = "time", "daynumber"
            frame = frame.join(mean_regime, on="daynumber")
            frame = frame.reset_index("daynumber", drop=True)

            # calculate anomaly
            frame.loc[:, "anomaly"] = (
                frame.loc[:, "value"] - frame.loc[:, "regime_mean"]
            )

            # plot
            if plot:
                plotdir = Path("plot")
                plotdir.mkdir(exist_ok=True)

                if plot_years is None:                    
                    plot_years = [today.year - 1, today.year]

                pngfile = plotdir / f"regime_{loc['name']}.png"
                plot_regime(
                    pngfile, series, mean_regime, loc=loc["name"], years=plot_years
                )

            # post
            if not post:
                continue
            for obs_type, label, field in timeseries_fields:
                old_ts_params = {
                    "pagesize": "1000000000",
                    "location__uuid": loc["uuid"],
                    "observation_type__id": f"{obs_type:d}",
                }
                get_old_ts_rs = requests.get(
                    url=ts_url, headers=headers, params=old_ts_params
                ).json()
                for ts in get_old_ts_rs["results"]:
                    del_ts_r = requests.delete(url=ts["url"], headers=headers)
                ts_data = {
                    "name": f"{loc['name']:}, regimecurve-{label:}",
                    "access_modifier": 0,  # public
                    "code": f"WNS9040.regime.{label:}::test5",
                    "supplier": username,
                    "location": loc["uuid"],
                    "supplier_code": None,
                    "value_type": 1,  # float
                    "frequency": None,
                    "observation_type": obs_type,
                    "timeseries_type": None,
                    "datasource": None,
                }
                post_ts_rs = requests.post(
                    url=ts_url + "/", data=json.dumps(ts_data), headers=headers
                ).json()
                log.info(post_ts_rs)

                evts_series = frame.loc[:, field].dropna()
                evts_data = data_records(evts_series)
                evts_url = lizardapi + "/timeseries/" + post_ts_rs["uuid"] + "/events"
                del_data_r = requests.delete(url=evts_url, headers=headers)
                post_data_r = requests.post(
                    url=evts_url + "/", data=json.dumps(evts_data), headers=headers
                )
                log.info(f"delete previous data: {del_data_r:}")
                log.info(f"post new data: {post_data_r:}")


def main():
    # arguments from input file
    args = get_parser().parse_args()
    with open(args.inputfile) as y:
        kwargs = yaml.load(y, yaml.SafeLoader)
    kwargs["inputfile"] = args.inputfile
    with open(args.credentialsfile) as y:
        kwargs["credentials"] = yaml.load(y, yaml.SafeLoader)
    kwargs["credentialsfile"] = args.credentialsfile
    run(**kwargs)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
