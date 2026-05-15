from __future__ import annotations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

import dash
import dash_bootstrap_components as dbc
from dash import Input, Output, callback, dash_table, dcc, html

from dashboard.data import (
    get_game_results,
    get_pitcher_matchups,
    get_player_stats,
    get_standings,
)

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
        dbc.Tab(label="📊  Player Stats",       tab_id="players"),
        dbc.Tab(label="🎮  Game Results",       tab_id="games"),
        dbc.Tab(label="⚔️   Pitcher vs Batter", tab_id="matchups"),
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
        title="Top 15 Batters by OPS",
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
        _section(f"All Batters (≥ 3 PA) — {len(df)} players"),
        dash_table.DataTable(
            data=df.to_dict("records"), columns=cols,
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


# ── Pitcher vs Batter tab ─────────────────────────────────────────────────────

def _matchups_tab() -> html.Div:
    df = get_pitcher_matchups()
    pitchers = sorted(df["pitcher_name"].dropna().unique().tolist())
    default  = pitchers[0] if pitchers else None

    return html.Div([
        dbc.Row(dbc.Col([
            html.Label("Select Pitcher", className="fw-bold small mb-1"),
            dcc.Dropdown(
                id="pitcher-select",
                options=[{"label": p, "value": p} for p in pitchers],
                value=default,
                clearable=False,
                className="mb-3",
            ),
        ], md=4)),
        dbc.Row([
            dbc.Col(dcc.Graph(id="matchup-chart",
                              config={"displayModeBar": False}), md=8),
            dbc.Col(html.Div(id="matchup-cards"), md=4),
        ]),
        _section("Matchup Detail"),
        html.Div(id="matchup-table"),
    ])


@callback(
    Output("matchup-chart", "figure"),
    Output("matchup-cards", "children"),
    Output("matchup-table", "children"),
    Input("pitcher-select", "value"),
)
def _update_matchups(pitcher: str | None):
    empty_fig = go.Figure().update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    if not pitcher:
        return empty_fig, html.Div(), html.Div()

    df   = get_pitcher_matchups()
    data = df[df["pitcher_name"] == pitcher].sort_values("batting_avg_vs", ascending=False)

    fig = px.bar(
        data.head(20),
        x="batter_name",
        y="batting_avg_vs",
        color="home_runs",
        color_continuous_scale="RdYlGn_r",
        labels={"batting_avg_vs": "AVG vs", "batter_name": "", "home_runs": "HR"},
        title=f"Batting Avg vs {pitcher}",
        text="batting_avg_vs",
        height=380,
    )
    fig.update_traces(texttemplate="%{text:.3f}", textposition="outside")
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        xaxis_tickangle=-40,
    )

    cards = dbc.Stack([
        _card("Batters Faced",   str(len(data)),                          "unique matchups"),
        _card("Opp Batting Avg", f"{data['batting_avg_vs'].mean():.3f}",  "avg allowed",   "warning"),
        _card("HR Allowed",      str(int(data["home_runs"].sum())),        "total HR",      "danger"),
        _card("Strikeouts",      str(int(data["strikeouts"].sum())),       "total K",       "success"),
    ], gap=2, className="mt-1")

    cols = [
        {"name": c, "id": i} for c, i in [
            ("Batter", "batter_name"), ("PA", "plate_appearances"),
            ("H", "hits"), ("HR", "home_runs"), ("K", "strikeouts"),
            ("BB", "walks"), ("RBI", "rbi"), ("AVG vs", "batting_avg_vs"),
        ]
    ]
    table = dash_table.DataTable(
        data=data.to_dict("records"), columns=cols,
        sort_action="native", page_size=15, **_TABLE_STYLE,
    )

    return fig, cards, table


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
        "games":     _games_tab,
        "matchups":  _matchups_tab,
    }
    return renderers.get(tab, lambda: html.Div())()


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8050)
