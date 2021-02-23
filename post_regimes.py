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


def setup_filelogging(dirname="log", level=logging.INFO):
    # create log directory
    logdir = Path(dirname)
    logdir.mkdir(exist_ok=True)

    # log file
    timestamp = pd.Timestamp.today().strftime("%Y%m%d")
    logfile = logdir / f"{timestamp:}.log"
    if logfile.exists():
        # remove today's old logs
        logfile.unlink()

    # filehandler    
    filehandler = logging.FileHandler(logfile)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    filehandler.setFormatter(formatter)
    filehandler.setLevel(level=level)

    # file logger
    log = logging.getLogger(os.path.basename(__file__))
    log.addHandler(filehandler)
    log.setLevel(level=level)

    return log


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
    regimes = kwargs["regimes"]
    plot = kwargs.get("plot", False)
    post = kwargs.get("post", False)
    plot_years = kwargs.get("plot_years")

    # log to file
    log = setup_filelogging()

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
    get_loc_rs = requests.get(url=loc_url, headers=headers, params=loc_params)
    try:
        get_loc_rs.raise_for_status()
    except Exception as e:
        log.exception("get locations: request failed")
        raise e
    locations_results = get_loc_rs.json()["results"]
    log.info(f"locations: {len(locations_results):d} results")
    for loc in locations_results:
        log.info(f"location \"{loc['name']:}\"")

        # get location timeseries
        ts_url = lizardapi + "/timeseries"
        ts_params = {
            "page_size": "1000000000",
            "name": "WNS9040",
            "location__uuid": loc["uuid"],
        }
        get_ts_rs = requests.get(url=ts_url, headers=headers, params=ts_params)
        try:
            get_ts_rs.raise_for_status()
        except Exception as e:
            log.exception("get timeseries: request failed")
            raise e
        timeseries_results = get_ts_rs.json()["results"]
        log.info(f"timeseries: {len(timeseries_results):d} results")
        for ts in timeseries_results:
            # skip if not in current year
            if ts["end"] is None:
                log.warning(f"skipping timeseries \"{ts['uuid']}\": no end value")
                continue
            ts_end = pd.to_datetime(ts["end"])
            if ts_end.year < today.year:
                log.warning(f"skipping timeseries \"{ts['uuid']}\": no values in this year")
                continue

            # get timeseries aggregate
            agg_url = lizardapi + "/timeseries/" + ts["uuid"] + "/aggregates"
            agg_params = {
                "page_size": "1000000000",
                "window": "day",
                "fields": "first_timestamp,avg",
                "start": start,
                "end": today,
            }
            get_agg_rs = requests.get(url=agg_url, headers=headers, params=agg_params)
            try:
                get_agg_rs.raise_for_status()
            except Exception as e:
                log.exception("get aggregates: request failed")
                raise e
            aggregates_results = get_agg_rs.json()["results"]
            series = get_timeseries(aggregates_results)

            log.info(f"series start: {series.index[0]:}")
            log.info(f"series end: {series.index[-1]:}")
            log.info(f"series length: {len(series):d} values")

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
                plotdir = Path("plot") / today.strftime("%Y%m%d")
                plotdir.mkdir(exist_ok=True, parents=True)

                if plot_years is None:
                    plot_years = [today.year - 1, today.year]

                pngfile = plotdir / f"regime_{loc['name']}.png"
                log.info(f"plot \"{pngfile:}\"")
                plot_regime(
                    pngfile, series, mean_regime, loc=loc["name"], years=plot_years
                )

            # post
            if not post:
                continue
            for regime in regimes:
                log.info(f"regime series \"{regime['name']:}\"")
                ex_reg_params = {
                    "pagesize": "1000000000",
                    "location__uuid": loc["uuid"],
                    "observation_type__id": f"{regime['observation_type']:d}",
                }
                get_ex_reg_rs = requests.get(
                    url=ts_url, headers=headers, params=ex_reg_params
                )
                try:
                    get_ex_reg_rs.raise_for_status()
                except Exception as e:
                    log.exception("get existing regime series: request failed")
                    raise e
                regime_results = get_ex_reg_rs.json()["results"]
                try:
                    regime_ts = regime_results[0]
                except IndexError:
                    log.info(f"new timeseries for \"{loc['name']:}\", \"{regime['name']:}\"")
                    # post new timeseries                   
                    regime_data = {
                        "name": f"{loc['name']:}, {regime['name']:}",
                        "access_modifier": 0,  # public
                        "code": regime["code"],
                        "supplier": credentials["username"],
                        "location": loc["uuid"],
                        "supplier_code": None,
                        "value_type": 1,  # float
                        "frequency": None,
                        "observation_type": regime["observation_type"],
                        "timeseries_type": None,
                        "datasource": None,
                    }
                    post_reg_rs = requests.post(
                        url=ts_url + "/", data=json.dumps(regime_data), headers=headers)
                    try:
                        post_reg_rs.raise_for_status()
                    except Exception as e:
                        log.exception("post regime series: request failed")                        
                    regime_ts = post_reg_rs.json()                

                # post data
                evts_series = frame.loc[:, regime["valuefield"]].dropna()
                evts_data = data_records(evts_series)
                evts_url = lizardapi + "/timeseries/" + regime_ts["uuid"] + "/events" 

                # delete existing data  
                log.info(f"delete existing data at \"{regime_ts['uuid']:}\"")             
                del_data_rs = requests.delete(url=evts_url, headers=headers)
                try:
                    del_data_rs.raise_for_status()
                except Exception as e:
                    log.exception("delete regime series data: request failed")
                    raise e

                # post new data
                log.info(f"post new data at \"{regime_ts['uuid']:}\"")  
                post_data_rs = requests.post(
                    url=evts_url + "/", data=json.dumps(evts_data), headers=headers
                )
                try:
                    post_data_rs.raise_for_status()
                except Exception as e:
                    log.exception("post regime series data: request failed")
                    raise e


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
