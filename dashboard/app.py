from __future__ import annotations

import pandas as pd
import plotly.express as px
from datetime import datetime

import dash
import dash_bootstrap_components as dbc
from dash import Input, Output, callback, dash_table, dcc, html

from dashboard.data import (
    get_game_results,
    get_pitching_leaders,
    get_player_stats,
    get_standings,
)
from dashboard.predictions import build_hr_leaderboard

# ── App ───────────────────────────────────────────────────────────────────────

app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.SLATE],
    title="MLB DiamondPipeline",
    suppress_callback_exceptions=True,
)
server = app.server  # exposed for gunicorn / Render deployment

DIVISION_COLORS = {
    "AL East":    "#4C78A8",
    "AL Central": "#F58518",
    "AL West":    "#54A24B",
    "NL East":    "#E45756",
    "NL Central": "#B279A2",
    "NL West":    "#EECA3B",
}

_TABLE_STYLE = dict(
    style_header={
        "backgroundColor": "#2b3035",
        "color": "white",
        "fontWeight": "bold",
        "border": "1px solid #495057",
    },
    style_data={
        "backgroundColor": "#343a40",
        "color": "white",
        "border": "1px solid #495057",
    },
    style_data_conditional=[
        {"if": {"row_index": "odd"}, "backgroundColor": "#3d4348"},
    ],
    style_table={"overflowX": "auto"},
    style_filter={"backgroundColor": "#2b3035", "color": "white"},
)

# ── Helpers ───────────────────────────────────────────────────────────────────

def _card(title: str, value: str, sub: str = "", color: str = "info") -> dbc.Card:
    return dbc.Card(
        dbc.CardBody([
            html.P(title, className="text-muted small mb-1"),
            html.H4(value, className=f"text-{color} fw-bold mb-0"),
            html.Small(sub, className="text-muted"),
        ]),
        className="h-100 border-secondary",
    )


def _section(text: str) -> html.H6:
    return html.H6(text, className="text-muted border-bottom pb-2 mt-4 mb-3")


# ── Layout ────────────────────────────────────────────────────────────────────

app.layout = dbc.Container([
    dcc.Interval(id="auto-refresh", interval=5 * 60 * 1000),

    # Header
    dbc.Row(dbc.Col(html.Div([
        html.Div([
            html.Span("⚾", style={"fontSize": "2rem", "lineHeight": "1"}),
            html.Span(" MLB DiamondPipeline", className="display-6 fw-bold ms-2"),
        ], className="d-flex align-items-center"),
        html.P(
            "Kafka · dbt · PostgreSQL · Airflow — live analytics pipeline",
            className="text-muted mb-0 small",
        ),
    ])), className="py-3 mb-3 border-bottom"),

    # Tabs
    dbc.Tabs(id="tabs", active_tab="standings", children=[
        dbc.Tab(label="🏆  Standings",         tab_id="standings"),
        dbc.Tab(label="🏃  Batting",            tab_id="players"),
        dbc.Tab(label="⚾  Pitching",           tab_id="pitching"),
        dbc.Tab(label="🎮  Game Results",       tab_id="games"),
        dbc.Tab(label="🔮  HR Predictions",     tab_id="hr_picks"),
    ]),
    html.Div(id="tab-content", className="mt-4"),

    # Footer
    html.Hr(className="mt-5 border-secondary"),
    html.P(
        f"MLB Stats API · Updated hourly via Airflow · "
        f"Server started {datetime.now().strftime('%b %d %Y %H:%M')}",
        className="text-muted text-center small pb-2",
    ),
], fluid=True, className="px-4 py-2")


# ── Standings tab ─────────────────────────────────────────────────────────────

def _standings_tab() -> html.Div:
    df = get_standings()
    if df.empty:
        return html.P("No standings data available.", className="text-muted")

    al = df[df["league_name"] == "American League"]
    nl = df[df["league_name"] == "National League"]
    al_top = al.nsmallest(1, "league_rank").iloc[0]
    nl_top = nl.nsmallest(1, "league_rank").iloc[0]

    fig = px.bar(
        df,
        x="team_name",
        y="win_pct",
        color="division_name",
        color_discrete_map=DIVISION_COLORS,
        facet_row="league_name",
        labels={"win_pct": "Win %", "team_name": "", "division_name": "Division"},
        title="Win Percentage by Team",
        height=560,
    )
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        xaxis_tickangle=-40,
        xaxis2_tickangle=-40,
        legend_title="Division",
    )
    fig.update_yaxes(tickformat=".3f", range=[0.25, 0.78])
    fig.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))

    cols = [
        {"name": c, "id": i} for c, i in [
            ("Team", "team_name"), ("Division", "division_name"),
            ("W", "wins"), ("L", "losses"), ("Win%", "win_pct"),
            ("GB", "games_back"), ("GP", "games_played"),
            ("Div Rank", "division_rank"), ("Lg Rank", "league_rank"),
        ]
    ]

    return html.Div([
        dbc.Row([
            dbc.Col(_card("AL Leader", al_top["team_name"],
                          f"{al_top['wins']}-{al_top['losses']}  ·  "
                          f"{al_top['win_pct']:.3f}", "info"), md=3),
            dbc.Col(_card("NL Leader", nl_top["team_name"],
                          f"{nl_top['wins']}-{nl_top['losses']}  ·  "
                          f"{nl_top['win_pct']:.3f}", "danger"), md=3),
            dbc.Col(_card("Total Teams", "30", "MLB franchises"), md=3),
            dbc.Col(_card("Season", str(int(df["season"].iloc[0])),
                          "current season"), md=3),
        ], className="g-3 mb-3"),

        dcc.Graph(figure=fig, config={"displayModeBar": False}),
        _section("Full Standings"),
        dash_table.DataTable(
            data=df.to_dict("records"), columns=cols,
            sort_action="native", filter_action="native",
            page_size=30, **_TABLE_STYLE,
        ),
    ])


# ── Player Stats tab ──────────────────────────────────────────────────────────

def _players_tab() -> html.Div:
    df = get_player_stats()
    if df.empty:
        return html.P("No player stats available.", className="text-muted")

    top_avg = df.nlargest(1, "batting_avg").iloc[0]
    top_hr  = df.nlargest(1, "home_runs").iloc[0]
    top_ops = df.nlargest(1, "ops").iloc[0]

    top15 = df.nlargest(15, "ops")
    fig = px.bar(
        top15.sort_values("ops"),
        x="ops",
        y="player_name",
        color="home_runs",
        orientation="h",
        color_continuous_scale="Blues",
        labels={"ops": "OPS", "player_name": "", "home_runs": "HR"},
        title="Top 15 Batters by OPS — 2026 Season (Official MLB Stats)",
        text="ops",
        height=420,
    )
    fig.update_traces(texttemplate="%{text:.3f}", textposition="outside")
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )

    cols = [
        {"name": c, "id": i} for c, i in [
            ("Player", "player_name"), ("G", "games"), ("PA", "plate_appearances"),
            ("H", "hits"), ("HR", "home_runs"), ("RBI", "rbi"),
            ("BB", "walks"), ("K", "strikeouts"),
            ("AVG", "batting_avg"), ("OBP", "obp"), ("SLG", "slg"), ("OPS", "ops"),
        ]
    ]

    return html.Div([
        dbc.Row([
            dbc.Col(_card("AVG Leader", top_avg["player_name"],
                          f"AVG {top_avg['batting_avg']:.3f}", "success"), md=4),
            dbc.Col(_card("HR Leader",  top_hr["player_name"],
                          f"{int(top_hr['home_runs'])} HR",    "warning"), md=4),
            dbc.Col(_card("OPS Leader", top_ops["player_name"],
                          f"OPS {top_ops['ops']:.3f}",         "info"),    md=4),
        ], className="g-3 mb-3"),

        dcc.Graph(figure=fig, config={"displayModeBar": False}),
        _section(f"All Batters (≥ 10 PA) — {len(df)} players · Official MLB 2026 Season Stats"),
        dash_table.DataTable(
            data=df.to_dict("records"), columns=cols,
            sort_action="native", filter_action="native",
            page_size=20, **_TABLE_STYLE,
        ),
    ])


# ── Pitching tab ─────────────────────────────────────────────────────────────

def _pitching_tab() -> html.Div:
    df = get_pitching_leaders()
    if df.empty:
        return html.P("No pitching stats available.", className="text-muted")

    starters  = df[df["role"] == "Starter"]
    relievers = df[df["role"] == "Reliever"]

    top_era  = starters.dropna(subset=["era"]).nsmallest(1, "era").iloc[0]  if not starters.empty  else None
    top_k    = df.nlargest(1, "strikeouts").iloc[0]                          if not df.empty         else None
    top_wins = starters.nlargest(1, "wins").iloc[0]                          if not starters.empty   else None
    top_sv   = relievers.nlargest(1, "saves").iloc[0]                        if not relievers.empty  else None

    top15_era = starters.dropna(subset=["era"]).nsmallest(15, "era")
    fig = px.bar(
        top15_era.sort_values("era", ascending=False),
        x="era",
        y="player_name",
        color="strikeouts",
        orientation="h",
        color_continuous_scale="Reds_r",
        labels={"era": "ERA", "player_name": "", "strikeouts": "K"},
        title="Top 15 Starters by ERA — 2026 Season (Official MLB Stats)",
        text="era",
        height=420,
    )
    fig.update_traces(texttemplate="%{text:.2f}", textposition="outside")
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )

    starter_cols = [
        {"name": c, "id": i} for c, i in [
            ("Pitcher", "player_name"), ("Team", "team_name"),
            ("W", "wins"), ("L", "losses"), ("G", "games"), ("GS", "games_started"),
            ("IP", "innings_pitched"), ("K", "strikeouts"), ("BB", "walks_allowed"),
            ("ERA", "era"), ("WHIP", "whip"), ("K/9", "k_per_9"),
        ]
    ]
    reliever_cols = [
        {"name": c, "id": i} for c, i in [
            ("Pitcher", "player_name"), ("Team", "team_name"),
            ("G", "games"), ("SV", "saves"), ("IP", "innings_pitched"),
            ("K", "strikeouts"), ("ERA", "era"), ("WHIP", "whip"),
        ]
    ]

    return html.Div([
        dbc.Row([
            dbc.Col(_card("ERA Leader",  top_era["player_name"]  if top_era  is not None else "-",
                          f"ERA {top_era['era']:.2f}"            if top_era  is not None else "", "success"), md=3),
            dbc.Col(_card("K Leader",   top_k["player_name"]    if top_k    is not None else "-",
                          f"{int(top_k['strikeouts'])} K"        if top_k    is not None else "", "info"),    md=3),
            dbc.Col(_card("Win Leader", top_wins["player_name"] if top_wins is not None else "-",
                          f"{int(top_wins['wins'])} W"           if top_wins is not None else "", "warning"), md=3),
            dbc.Col(_card("Save Leader",top_sv["player_name"]   if top_sv   is not None else "-",
                          f"{int(top_sv['saves'])} SV"           if top_sv   is not None else "", "danger"),  md=3),
        ], className="g-3 mb-3"),

        dcc.Graph(figure=fig, config={"displayModeBar": False}),

        _section(f"Starters — {len(starters)} pitchers"),
        dash_table.DataTable(
            data=starters.to_dict("records"), columns=starter_cols,
            sort_action="native", filter_action="native",
            page_size=20, **_TABLE_STYLE,
        ),

        _section(f"Relievers — {len(relievers)} pitchers"),
        dash_table.DataTable(
            data=relievers.to_dict("records"), columns=reliever_cols,
            sort_action="native", filter_action="native",
            page_size=20, **_TABLE_STYLE,
        ),
    ])


# ── Game Results tab ──────────────────────────────────────────────────────────

def _games_tab() -> html.Div:
    df = get_game_results()
    if df.empty:
        return html.P("No completed games available.", className="text-muted")

    wins = (
        df["winning_team"].value_counts()
        .reset_index()
        .rename(columns={"winning_team": "team", "count": "wins"})
    )

    fig = px.bar(
        wins.sort_values("wins"),
        x="wins", y="team",
        orientation="h",
        color="wins",
        color_continuous_scale="Greens",
        labels={"wins": "Wins", "team": ""},
        title="Wins from Available Game Data",
        text="wins",
        height=380,
    )
    fig.update_traces(textposition="outside")
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        showlegend=False,
    )

    display = df.copy()
    display["game_date"] = pd.to_datetime(display["game_date"]).dt.strftime("%Y-%m-%d")
    display["score"]     = (
        display["away_score"].fillna(0).astype(int).astype(str) + " – " +
        display["home_score"].fillna(0).astype(int).astype(str)
    )
    avg_runs = (df["home_score"].fillna(0) + df["away_score"].fillna(0)).mean()

    cols = [
        {"name": c, "id": i} for c, i in [
            ("Date", "game_date"), ("Away", "away_team_name"), ("Home", "home_team_name"),
            ("Score", "score"), ("Winner", "winning_team"),
            ("Run Diff", "run_differential"), ("Venue", "venue_name"),
        ]
    ]

    return html.Div([
        dbc.Row([
            dbc.Col(_card("Games",       str(len(df)), "completed"),       md=3),
            dbc.Col(_card("Avg Runs",    f"{avg_runs:.1f}", "per game"),   md=3),
            dbc.Col(_card("Biggest Win",
                          f"{int(df['run_differential'].max())} runs",
                          df.nlargest(1, "run_differential").iloc[0]["winning_team"],
                          "success"), md=3),
            dbc.Col(_card("Closest Game",
                          f"{int(df['run_differential'].min())} run",
                          df.nsmallest(1, "run_differential").iloc[0]["winning_team"],
                          "warning"), md=3),
        ], className="g-3 mb-3"),

        dcc.Graph(figure=fig, config={"displayModeBar": False}),
        _section("Game Log"),
        dash_table.DataTable(
            data=display.to_dict("records"), columns=cols,
            sort_action="native", page_size=15, **_TABLE_STYLE,
        ),
    ])


# ── HR Predictions tab ───────────────────────────────────────────────────────

def _hr_picks_tab() -> html.Div:
    df = build_hr_leaderboard()

    if df.empty:
        return html.Div([
            html.P(
                "Probable pitchers have not been announced for today's games yet. "
                "Check back closer to game time.",
                className="text-muted mt-4",
            )
        ])

    top       = df.iloc[0]
    games     = df["matchup"].nunique()
    best_park = df.nlargest(1, "park_factor").iloc[0]
    avg_prob  = df["hr_prob"].mean()

    top20 = df.head(20)
    fig = px.bar(
        top20.sort_values("hr_prob"),
        x="hr_prob",
        y="batter_name",
        color="park_factor",
        orientation="h",
        color_continuous_scale="RdYlGn",
        range_color=[0.80, 1.40],
        labels={"hr_prob": "HR Probability", "batter_name": "", "park_factor": "Park Factor"},
        title="Top 20 HR Candidates Today",
        text=top20.sort_values("hr_prob")["hr_prob_pct"],
        height=520,
    )
    fig.update_traces(textposition="outside")
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        xaxis_tickformat=".1%",
        coloraxis_colorbar_title="Park<br>Factor",
    )

    cols = [
        {"name": c, "id": i} for c, i in [
            ("Batter",       "batter_name"),
            ("Team",         "team"),
            ("Vs Pitcher",   "pitcher"),
            ("Venue",        "venue"),
            ("Park Factor",  "park_factor"),
            ("HR Prob",      "hr_prob_pct"),
            ("Season HR",    "season_hr"),
            ("Season PA",    "season_pa"),
            ("AVG",          "batting_avg"),
            ("OPS",          "ops"),
        ]
    ]

    return html.Div([
        dbc.Row([
            dbc.Col(_card("Top Candidate",
                          top["batter_name"],
                          f"{top['hr_prob_pct']} · {top['matchup']}", "warning"), md=3),
            dbc.Col(_card("Games Today",
                          str(games),
                          "with probable pitchers announced"), md=3),
            dbc.Col(_card("Best Park Today",
                          best_park["venue"].split()[-1],   # last word e.g. "Field"
                          f"Park factor {best_park['park_factor']:.2f}×", "success"), md=3),
            dbc.Col(_card("Avg HR Probability",
                          f"{avg_prob * 100:.1f}%",
                          f"across {len(df)} qualifying matchups"), md=3),
        ], className="g-3 mb-3"),

        dcc.Graph(figure=fig, config={"displayModeBar": False}),

        _section(f"Full Leaderboard — {len(df)} batter matchups"),
        dash_table.DataTable(
            data=df.to_dict("records"),
            columns=cols,
            sort_action="native",
            filter_action="native",
            page_size=25,
            **_TABLE_STYLE,
        ),

        html.Hr(className="border-secondary mt-4"),
        html.P(
            "📐 Model: season HR rate × pitcher HR allowed rate ÷ league average (3.4% per PA) × park factor. "
            "Probable pitchers from MLB Stats API. Batters with fewer than 20 PA excluded. "
            "Park factors based on historical multi-year HR rates. Platoon adjustments (L/R) coming soon.",
            className="text-muted small",
        ),
    ])


# ── Tab router ────────────────────────────────────────────────────────────────

@callback(
    Output("tab-content", "children"),
    Input("tabs", "active_tab"),
    Input("auto-refresh", "n_intervals"),
)
def _render_tab(tab: str, _: int) -> html.Div:
    renderers = {
        "standings": _standings_tab,
        "players":   _players_tab,
        "pitching":  _pitching_tab,
        "games":     _games_tab,
        "hr_picks":  _hr_picks_tab,
    }
    return renderers.get(tab, lambda: html.Div())()


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8050)
