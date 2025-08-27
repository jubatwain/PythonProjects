import urllib.request
import json
from pulp import *

# Fetch static data (players, teams, gameweeks)
bootstrap_url = "https://fantasy.premierleague.com/api/bootstrap-static/"
with urllib.request.urlopen(bootstrap_url) as response:
    bootstrap_data = json.loads(response.read().decode())

players = bootstrap_data['elements']
teams = bootstrap_data['teams']
events = bootstrap_data['events']

# Next gameweek
next_gw = next((event['id'] for event in events if event['is_next']), None)
if next_gw is None:
    print("No upcoming gameweek found.")
    exit()

# Fetch fixtures for next gameweek
fixtures_url = f"https://fantasy.premierleague.com/api/fixtures/?event={next_gw}"
with urllib.request.urlopen(fixtures_url) as response:
    fixtures = json.loads(response.read().decode())

# Group fixtures by team: team_id -> list of {'opp_id': int, 'diff': int, 'home': bool}
team_fixtures = {t['id']: [] for t in teams}
for f in fixtures:
    team_fixtures[f['team_h']].append({'opp_id': f['team_a'], 'diff': f['team_h_difficulty'], 'home': True})
    team_fixtures[f['team_a']].append({'opp_id': f['team_h'], 'diff': f['team_a_difficulty'], 'home': False})

# Average team strength for normalization
avg_strength = sum(t['strength'] for t in teams) / len(teams)

# Compute expected points for available players
expected_points = {}
for player in players:
    team_id = player['team']
    fixes = team_fixtures[team_id]
    if not fixes:
        continue  # Blank gameweek

    chance = player['chance_of_playing_next_round']
    if chance == 0:
        continue  # Injured/suspended/unavailable
    chance_factor = (chance if chance is not None else 100) / 100.0

    base = (float(player['points_per_game']) * 0.4 + float(player['form']) * 0.6)  # Weight form higher
    ict_boost = float(player['ict_index']) / 100 * 0.2  # Small boost for impact

    exp = 0.0
    for fix in fixes:
        opp_id = fix['opp_id']
        opp_strength = next(t['strength'] for t in teams if t['id'] == opp_id)
        strength_factor = avg_strength / opp_strength  # >1 for weaker opponents
        diff_factor = (6 - fix['diff']) / 5.0
        home_bonus = 1.2 if fix['home'] else 0.9
        fix_exp = base * diff_factor * home_bonus * strength_factor * (1 + ict_boost)
        exp += fix_exp

    exp *= chance_factor
    expected_points[player['id']] = round(exp, 2)

    # Store for later use
    player['expected_points'] = expected_points[player['id']]
    player['fixtures'] = fixes  # For reasons
    player['chance'] = chance if chance is not None else 100
    player['form_val'] = float(player['form'])
    player['ppg_val'] = float(player['points_per_game'])
    player['ict_val'] = float(player['ict_index'])

# Filter available players
avail_players = [p for p in players if p['id'] in expected_points]

# Squad Optimization (full rebuild, e.g., wildcard)
prob = LpProblem("FPL_Squad", LpMaximize)
select = LpVariable.dicts("Select", (p['id'] for p in avail_players), cat='Binary')
prob += lpSum([select[p['id']] * expected_points[p['id']] for p in avail_players])
prob += lpSum([select[p['id']] * (p['now_cost'] / 10.0) for p in avail_players]) <= 100.0  # Budget 100m
prob += lpSum(select[p['id']] for p in avail_players) == 15  # Squad size
pos_counts = {1: 2, 2: 5, 3: 5, 4: 3}  # GK, DEF, MID, FWD
for pos, count in pos_counts.items():
    prob += lpSum([select[p['id']] for p in avail_players if p['element_type'] == pos]) == count
for team_id in range(1, 21):
    prob += lpSum([select[p['id']] for p in avail_players if p['team'] == team_id]) <= 3
prob.solve(PULP_CBC_CMD(msg=0))

if LpStatus[prob.status] != 'Optimal':
    print("No optimal squad found.")
    exit()

selected = [p for p in avail_players if select[p['id']].value() == 1]
total_cost = sum(p['now_cost'] / 10.0 for p in selected)
squad_expected = sum(p['expected_points'] for p in selected)

# Starting 11 Optimization: Test formations and pick best (incl. captain double)
formations = [(3,5,2), (3,4,3), (4,4,2), (4,3,3), (4,5,1), (5,4,1), (5,3,2)]
best_form = None
best_proj = 0
best_lineup = []
best_captain = None
best_vice = None
for def_c, mid_c, fwd_c in formations:
    prob2 = LpProblem("FPL_Lineup", LpMaximize)
    select2 = LpVariable.dicts("Select2", (p['id'] for p in selected), cat='Binary')
    prob2 += lpSum([select2[p['id']] * expected_points[p['id']] for p in selected])
    prob2 += lpSum(select2[p['id']] for p in selected) == 11
    prob2 += lpSum([select2[p['id']] for p in selected if p['element_type'] == 1]) == 1  # 1 GK
    prob2 += lpSum([select2[p['id']] for p in selected if p['element_type'] == 2]) == def_c
    prob2 += lpSum([select2[p['id']] for p in selected if p['element_type'] == 3]) == mid_c
    prob2 += lpSum([select2[p['id']] for p in selected if p['element_type'] == 4]) == fwd_c
    prob2.solve(PULP_CBC_CMD(msg=0))

    if LpStatus[prob2.status] == 'Optimal':
        lineup = [p for p in selected if select2[p['id']].value() == 1]
        lineup_sum = sum(p['expected_points'] for p in lineup)
        exps = sorted([p['expected_points'] for p in lineup], reverse=True)
        cap_bonus = exps[0] if exps else 0  # Captain double
        proj_total = lineup_sum + cap_bonus
        if proj_total > best_proj:
            best_proj = proj_total
            best_form = f"{def_c}-{mid_c}-{fwd_c}"
            best_lineup = lineup
            best_captain = max(lineup, key=lambda p: p['expected_points'])
            best_vice = sorted(lineup, key=lambda p: p['expected_points'], reverse=True)[1]

# Bench: Remaining, sorted by expected descending
bench = sorted([p for p in selected if p not in best_lineup], key=lambda p: p['expected_points'], reverse=True)

# Helper to get team name
def get_team_name(tid):
    return next(t['short_name'] for t in teams if t['id'] == tid)

# Output
print(f"Optimal Team for Gameweek {next_gw} (Total Cost: {total_cost}m, Squad Expected Points: {squad_expected}, Lineup Projected Points with Captain: {best_proj})")
print(f"Recommended Formation: {best_form}")
print("\nStarting 11:")

positions = {1: 'GK', 2: 'DEF', 3: 'MID', 4: 'FWD'}
for pos in [1, 2, 3, 4]:
    pos_players = sorted([p for p in best_lineup if p['element_type'] == pos], key=lambda p: p['expected_points'], reverse=True)
    if pos_players:
        print(f"\n{positions[pos]}:")
        for p in pos_players:
            price = p['now_cost'] / 10.0
            team_name = get_team_name(p['team'])
            fix_str = ', '.join(f"vs {get_team_name(f['opp_id'])} ({'H' if f['home'] else 'A'}, diff {f['diff']})" for f in p['fixtures'])
            reason = f"Selected for high expected {p['expected_points']} pts (form {p['form_val']}, PPG {p['ppg_val']}, ICT {p['ict_val']}); favorable fixtures: {fix_str}; chance {p['chance']}%; strong vs opponent strength."
            print(f"- {p['web_name']} ({team_name}, {price}m, Pos: {positions[pos]}) - Expected: {p['expected_points']} - Reason: {reason}")
            if p == best_captain:
                print("  (Captain - Double points potential)")
            elif p == best_vice:
                print("  (Vice-Captain)")

print("\nBench (in priority order):")
for p in bench:
    price = p['now_cost'] / 10.0
    team_name = get_team_name(p['team'])
    pos_name = positions[p['element_type']]
    fix_str = ', '.join(f"vs {get_team_name(f['opp_id'])} ({'H' if f['home'] else 'A'}, diff {f['diff']})" for f in p['fixtures'])
    reason = f"Backup with {p['expected_points']} pts potential (form {p['form_val']}, PPG {p['ppg_val']}); fixtures: {fix_str}; chance {p['chance']}%."
    print(f"- {p['web_name']} ({team_name}, {price}m, Pos: {pos_name}) - Expected: {p['expected_points']} - Reason: {reason}")

print("\nWhat you stand to gain: This team maximizes projected points by balancing form, fixtures, and value. Use wildcard if needed for full changes; otherwise, adapt for limited transfers.")