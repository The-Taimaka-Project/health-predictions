from datetime import timedelta

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def plot_forecast(
    df,
    muac_trajectories,
    wfhz_trajectories,
    path="images",
    switch_week=3,
    discharge_days=None,
    xlim=[0, 90],
    muac_ylim=[7, 14],
    wfhz_ylim=[-6, 1],
    forecast_end_day=90,
    q=1.645,
):
    df = df.copy()
    if "status_granular" in df.columns:
        df = df.drop(columns="status_granular")

    for n in range(1, df.shape[0] + 1):
        current_day = df[:n]["days_in_program"].max()

        # create the recovery trajectories
        muac_recovery_df = muac_trajectories.create_recovery_trajectory(
            df[: min(n, switch_week)].copy()
        )
        muac_recovery = muac_recovery_df["forecast"]
        muac_recovery_sd = muac_recovery_df["std"]

        wfhz_recovery_df = wfhz_trajectories.create_recovery_trajectory(
            df[: min(n, switch_week)].copy()
        )
        wfhz_recovery = wfhz_recovery_df["forecast"]
        wfhz_recovery_sd = wfhz_recovery_df["std"]

        fig, ax = plt.subplots(1, 2, figsize=(12, 4))

        ax[0].fill_between(
            muac_recovery_df["days"],
            muac_recovery - q * muac_recovery_sd,
            muac_recovery + q * muac_recovery_sd,
            color="blue",
            alpha=0.1,
            linewidth=0,
        )

        if n > switch_week:

            # create the forecasted trajectories
            muac_forecast_df = muac_trajectories.forecast(
                df[:n].copy(), forecast_end_day=forecast_end_day
            )
            wfhz_forecast_df = wfhz_trajectories.forecast(
                df[:n].copy(), forecast_end_day=forecast_end_day
            )

            muac_forecast = muac_forecast_df["forecast"].values
            muac_forecast_sd = muac_forecast_df["std"].values
            wfhz_forecast = wfhz_forecast_df["forecast"].values
            wfhz_forecast_sd = wfhz_forecast_df["std"].values

            # set current forecasts to current observations
            ax[0].fill_between(
                muac_forecast_df["days"],
                muac_forecast - q * muac_forecast_sd,
                muac_forecast + q * muac_forecast_sd,
                color="orange",
                alpha=0.1,
                linewidth=0,
            )
            ax[0].plot(
                muac_forecast_df["days"],
                muac_forecast,
                color="orange",
                label="forecasted",
            )

        ax[0].plot(
            df["days_in_program"][:n],
            df["muac_weekly"][:n],
            color="black",
            label="observed",
            linestyle="dashed",
        )
        ax[0].scatter(df["days_in_program"][:n], df["muac_weekly"][:n], color="black")
        ax[0].plot(
            muac_recovery_df["days"],
            muac_recovery,
            color="blue",
            label="recovery",
        )
        ax[0].axhline(12.5, color="grey", linestyle="dashed")

        # specify order of legend
        order = [0, 1] if n <= switch_week else [1, 2, 0]

        if discharge_days is not None:
            for i, ds in enumerate(discharge_days):
                if current_day >= ds[0]:
                    ax[0].axvspan(
                        ds[0],
                        min(ds[1], current_day),
                        alpha=0.1,
                        color="red",
                        linewidth=0,
                        label="in ITP" if i == 0 else None,
                    )
                    order = [0, 1, 2] if n <= switch_week else [1, 2, 0, 3]

        ax[0].set_ylabel("MUAC")

        # reordering the labels
        handles, labels = ax[0].get_legend_handles_labels()

        # pass handle & labels lists along with order as below
        ax[0].legend(
            [handles[i] for i in order],
            [labels[i] for i in order],
            title=f"PID {df['pid'].unique().item()}",
            loc="lower right",
        )

        ax[0].set_xlim(xlim[0], xlim[1])
        ax[0].set_ylim(muac_ylim[0], muac_ylim[1])

        # set ticklabels
        start_date = df["calcdate_weekly"].min()
        tick_position = np.arange(0, muac_recovery_df["days"].max(), 21)
        date_ticks = [start_date + timedelta(days=i) for i in tick_position]
        tick_labels = [d.strftime("%d %b") for d in date_ticks]
        tick_labels[0] = date_ticks[0].strftime("%d %b '%y")
        ax[0].set_xticks(tick_position, tick_labels)

        ax[1].fill_between(
            wfhz_recovery_df["days"],
            wfhz_recovery - q * wfhz_recovery_sd,
            wfhz_recovery + q * wfhz_recovery_sd,
            color="blue",
            alpha=0.1,
            linewidth=0,
        )

        if n > switch_week:
            ax[1].fill_between(
                wfhz_forecast_df["days"],
                wfhz_forecast - q * wfhz_forecast_sd,
                wfhz_forecast + q * wfhz_forecast_sd,
                color="orange",
                alpha=0.1,
                linewidth=0,
            )
            ax[1].plot(
                wfhz_forecast_df["days"],
                wfhz_forecast,
                color="orange",
            )

        ax[1].plot(df["days_in_program"][:n], df["wfhz"][:n], color="black", linestyle="dashed")
        ax[1].scatter(df["days_in_program"][:n], df["wfhz"][:n], color="black")
        ax[1].plot(
            wfhz_recovery_df["days"],
            wfhz_recovery,
            color="blue",
        )
        if discharge_days is not None:
            for ds in discharge_days:
                if current_day >= ds[0]:
                    ax[1].axvspan(
                        ds[0],
                        min(ds[1], current_day),
                        alpha=0.1,
                        color="red",
                        linewidth=0,
                    )
        ax[1].axhline(-2, color="grey", linestyle="dashed")
        ax[1].set_ylabel("WFHZ")
        ax[1].set_xlim(xlim[0], xlim[1])
        ax[1].set_ylim(wfhz_ylim[0], wfhz_ylim[1])

        # set ticklabels
        start_date = df["calcdate_weekly"].min()
        tick_position = np.arange(0, wfhz_recovery_df["days"].max(), 21)
        date_ticks = [start_date + timedelta(days=i) for i in tick_position]
        tick_labels = [d.strftime("%d %b") for d in date_ticks]
        tick_labels[0] = date_ticks[0].strftime("%d %b '%y")
        ax[1].set_xticks(tick_position, tick_labels)

        label = "Visit" if n == 1 else "Visits"
        plt.suptitle(f"{n} {label}")
        plt.savefig(f"{path}/{str(n).zfill(2)}.png", dpi=300, bbox_inches="tight")
        plt.close(fig)
