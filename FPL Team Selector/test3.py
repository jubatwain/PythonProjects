import os
import json
import urllib.request
import argparse
from pulp import LpProblem, LpMaximize, LpVariable, LpStatus, PULP_CBC_CMD, lpSum
import logging

# Constants
SQUAD_FILE = 'current_squad.json'
LOG_FILE = 'fpl_optimizer.log'

# Configure logging
logging.basicConfig(filename=LOG_FILE, level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


def load_squad():
    """Load the current squad from a JSON file, returning an empty list if not found."""
    if os.path.exists(SQUAD_FILE):
        try:
            with open(SQUAD_FILE, 'r') as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            logging.error(f"Error decoding squad file {SQUAD_FILE}: {e}")
            return []
    return []


def save_squad(selected):
    """Save the optimized squad to a JSON file."""
    optimized_squad = [{'id': p['id'], 'web_name': p['web_name'], 'element_type': p['element_type'], 'team': p['team']}
                       for p in selected]
    try:
        with open(SQUAD_FILE, 'w') as f:
            json.dump(optimized_squad, f, indent=2)
    except Exception as e:
        logging.error(f"Error saving squad to {SQUAD_FILE}: {e}")


def fetch_data():
    """Fetch player, team, and fixture data from FPL API for the next gameweek."""
    try:
        bootstrap_url = "https://fantasy.premierleague.com/api/bootstrap-static/"
        with urllib.request.urlopen(bootstrap_url) as response:
            bootstrap_data = json.loads(response.read().decode())
        # Fetch fixtures for the next gameweek dynamically
        events = bootstrap_data['events']
        next_gw = get_next_gameweek(events)
        fixtures_url = f"https://fantasy.premierleague.com/api/fixtures/?event={next_gw}"
        with urllib.request.urlopen(fixtures_url) as response:
            fixtures = json.loads(response.read().decode())
        return bootstrap_data['elements'], bootstrap_data['teams'], events, fixtures, next_gw
    except Exception as e:
        logging.error(f"Error fetching data: {e}")
        raise


def get_next_gameweek(events):
    """Identify the next gameweek."""
    next_gw = next((event['id'] for event in events if event['is_next']), None)
    if next_gw is None:
        logging.warning("No 'is_next' gameweek found, using highest finished gameweek + 1.")
        finished_gw = max((event['id'] for event in events if event['finished']), default=0)
        next_gw = finished_gw + 1
    return next_gw


def update_fixtures(next_gw):
    """Fetch fixtures for the specified gameweek."""
    try:
        fixtures_url = f"https://fantasy.premierleague.com/api/fixtures/?event={next_gw}"
        with urllib.request.urlopen(fixtures_url) as response:
            return json.loads(response.read().decode())
    except Exception as e:
        logging.error(f"Error fetching fixtures: {e}")
        raise


def calculate_team_fixtures(teams, fixtures):
    """Group fixtures by team."""
    team_fixtures = {t['id']: [] for t in teams}
    for f in fixtures:
        team_fixtures[f['team_h']].append({'opp_id': f['team_a'], 'diff': f['team_h_difficulty'], 'home': True})
        team_fixtures[f['team_a']].append({'opp_id': f['team_h'], 'diff': f['team_a_difficulty'], 'home': False})
    return team_fixtures


def calculate_expected_points(players, teams, team_fixtures):
    """Calculate expected points for each player based on form, fixtures, and scoring rules."""
    avg_strength = sum(t['strength'] for t in teams) / len(teams)
    expected_points = {}
    team_strength = {t['id']: t['strength'] for t in teams}

    for player in players:
        team_id = player['team']
        fixes = team_fixtures.get(team_id, [])
        if not fixes:
            continue
        chance = player.get('chance_of_playing_next_round', 100)  # Default to 100 if None
        if chance == 0:
            continue
        # Ensure chance is a number before division
        try:
            chance_factor = float(chance) / 100.0
        except (TypeError, ValueError) as e:
            logging.warning(
                f"Invalid chance value for player {player['id']} ({player['web_name']}): {chance}. Setting to 1.0.")
            chance_factor = 1.0

        base = (float(player.get('form', 0)) * 0.6 + float(player.get('points_per_game', 0)) * 0.4)
        ict_boost = float(player.get('ict_index', 0)) / 100 * 0.2

        exp = 0.0
        for fix in fixes:
            opp_id = fix['opp_id']
            opp_strength = team_strength.get(opp_id, avg_strength)
            strength_factor = avg_strength / opp_strength
            diff_factor = (6 - fix['diff']) / 5.0
            home_bonus = 1.2 if fix['home'] else 0.9
            fix_exp = base * diff_factor * home_bonus * strength_factor * (1 + ict_boost)
            exp += fix_exp

        exp *= chance_factor
        expected_points[player['id']] = round(exp, 2)
        player['expected_points'] = expected_points[player['id']]
        player['fixtures'] = fixes
        player['chance'] = chance
        player['form_val'] = float(player.get('form', 0))
        player['ppg_val'] = float(player.get('points_per_game', 0))
        player['ict_val'] = float(player.get('ict_index', 0))

    return expected_points


def optimize_squad(players, expected_points, current_squad, free_transfers, chip):
    """Optimize a 15-player squad, considering transfers if provided."""
    avail_players = [p for p in players if p['id'] in expected_points]
    prob = LpProblem("FPL_Squad", LpMaximize)

    # Map current squad to full player objects
    current_squad_players = []
    if current_squad:
        for squad_player in current_squad:
            if isinstance(squad_player, dict) and 'id' in squad_player:
                matching_players = [p for p in avail_players if p['id'] == squad_player['id']]
                if matching_players:
                    current_squad_players.append(matching_players[0])
                else:
                    logging.warning(f"Player with ID {squad_player['id']} not found in current data. Skipping.")
            else:
                logging.warning(f"Invalid squad player format: {squad_player}. Skipping.")

    # Variables for transfers
    keep = LpVariable.dicts("Keep", (p['id'] for p in current_squad_players),
                            cat='Binary') if current_squad_players else {}
    buy = LpVariable.dicts("Buy", (p['id'] for p in avail_players if p not in current_squad_players),
                           cat='Binary') if current_squad_players else LpVariable.dicts("Buy", (p['id'] for p in
                                                                                                avail_players),
                                                                                        cat='Binary')

    # Objective
    if current_squad_players and chip not in ['wildcard', 'free_hit']:
        extra_transfers = LpVariable("ExtraTransfers", lowBound=0, cat='Integer')
        prob += lpSum([keep[p['id']] * expected_points[p['id']] for p in current_squad_players]) + lpSum(
            [buy[p['id']] * expected_points[p['id']] for p in avail_players if
             p not in current_squad_players]) - 4 * extra_transfers
        prob += lpSum([1 - keep[p['id']] for p in current_squad_players]) <= free_transfers + extra_transfers
    else:
        select = LpVariable.dicts("Select", (p['id'] for p in avail_players), cat='Binary')
        prob += lpSum([select[p['id']] * expected_points[p['id']] for p in avail_players])

    # Constraints
    if current_squad_players and chip not in ['wildcard', 'free_hit']:
        prob += lpSum(keep.values()) + lpSum(buy.values()) == 15
        prob += lpSum([1 - keep[p['id']] for p in current_squad_players]) == lpSum(buy.values())
        sold_value = lpSum([(1 - keep[p['id']]) * (p['now_cost'] / 10.0) for p in current_squad_players])
        buy_cost = lpSum(
            [buy[p['id']] * (p['now_cost'] / 10.0) for p in avail_players if p not in current_squad_players])
        prob += buy_cost <= sold_value
    else:
        prob += lpSum(select.values()) == 15
        prob += lpSum([select[p['id']] * (p['now_cost'] / 10.0) for p in avail_players]) <= 100.0

    # Position and team constraints
    pos_counts = {1: 2, 2: 5, 3: 5, 4: 3}
    if current_squad_players and chip not in ['wildcard', 'free_hit']:
        for pos, count in pos_counts.items():
            prob += lpSum([keep[p['id']] for p in current_squad_players if p['element_type'] == pos]) + lpSum(
                [buy[p['id']] for p in avail_players if
                 p not in current_squad_players and p['element_type'] == pos]) == count
        for team_id in range(1, 21):
            prob += lpSum([keep[p['id']] for p in current_squad_players if p['team'] == team_id]) + lpSum(
                [buy[p['id']] for p in avail_players if p not in current_squad_players and p['team'] == team_id]) <= 3
    else:
        for pos, count in pos_counts.items():
            prob += lpSum([select[p['id']] for p in avail_players if p['element_type'] == pos]) == count
        for team_id in range(1, 21):
            prob += lpSum([select[p['id']] for p in avail_players if p['team'] == team_id]) <= 3

    prob.solve(PULP_CBC_CMD(msg=0))
    if LpStatus[prob.status] != 'Optimal':
        logging.error("No optimal squad found.")
        raise ValueError("No optimal squad.")

    if current_squad_players and chip not in ['wildcard', 'free_hit']:
        selected = [p for p in current_squad_players if keep[p['id']].value() == 1] + [p for p in avail_players if
                                                                                       p not in current_squad_players and buy.get(
                                                                                           p['id'], 0).value() == 1]
    else:
        selected = [p for p in avail_players if select[p['id']].value() == 1]
    total_cost = sum(p['now_cost'] / 10.0 for p in selected)
    squad_expected = sum(p['expected_points'] for p in selected)
    return selected, total_cost, squad_expected


def optimize_lineup(selected, expected_points, chip):
    """Optimize starting 11 and select captain/vice-captain."""
    formations = [(3, 5, 2), (3, 4, 3), (4, 4, 2), (4, 3, 3), (4, 5, 1), (5, 4, 1), (5, 3, 2)]
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
        prob2 += lpSum([select2[p['id']] for p in selected if p['element_type'] == 1]) == 1
        prob2 += lpSum([select2[p['id']] for p in selected if p['element_type'] == 2]) == def_c
        prob2 += lpSum([select2[p['id']] for p in selected if p['element_type'] == 3]) == mid_c
        prob2 += lpSum([select2[p['id']] for p in selected if p['element_type'] == 4]) == fwd_c
        prob2.solve(PULP_CBC_CMD(msg=0))

        if LpStatus[prob2.status] == 'Optimal':
            lineup = [p for p in selected if select2[p['id']].value() == 1]
            lineup_sum = sum(p['expected_points'] for p in lineup)
            exps = sorted([p['expected_points'] for p in lineup], reverse=True)
            cap_bonus = exps[0] * (2 if chip != 'triple_captain' else 3) if exps else 0
            proj_total = lineup_sum + cap_bonus
            if chip == 'bench_boost':
                proj_total += sum(p['expected_points'] for p in selected if p not in lineup)
            if proj_total > best_proj:
                best_proj = proj_total
                best_form = f"{def_c}-{mid_c}-{fwd_c}"
                best_lineup = lineup
                best_captain = max(lineup, key=lambda p: p['expected_points'])
                best_vice = sorted(lineup, key=lambda p: p['expected_points'], reverse=True)[1] if len(
                    lineup) > 1 else None

    return best_lineup, best_form, best_proj, best_captain, best_vice


def get_team_name(teams, tid):
    """Get team short name from ID."""
    return next((t['short_name'] for t in teams if t['id'] == tid), "Unknown")


def print_output(next_gw, selected, total_cost, squad_expected, best_lineup, best_form, best_proj, best_captain,
                 best_vice, bench, teams, chip):
    """Print optimized squad, lineup, and bench."""
    print(
        f"Optimal Team for Gameweek {next_gw} (Total Cost: £{total_cost:.1f}, Squad Expected Points: {squad_expected:.2f}, Lineup Projected Points: {best_proj:.2f})")
    if chip:
        print(f"Chip Active: {chip.replace('_', ' ').title()}")
    print(f"Recommended Formation: {best_form}")

    positions = {1: 'GK', 2: 'DEF', 3: 'MID', 4: 'FWD'}
    print("\nStarting 11:")
    for pos in [1, 2, 3, 4]:
        pos_players = sorted([p for p in best_lineup if p['element_type'] == pos], key=lambda p: p['expected_points'],
                             reverse=True)
        if pos_players:
            print(f"\n{positions[pos]}:")
            for p in pos_players:
                price = p['now_cost'] / 10.0
                team_name = get_team_name(teams, p['team'])
                fix_str = ', '.join(
                    f"vs {get_team_name(teams, f['opp_id'])} ({'H' if f['home'] else 'A'}, diff {f['diff']})" for f in
                    p['fixtures'])
                reason = f"Selected for {p['expected_points']} pts (form {p['form_val']:.1f}, PPG {p['ppg_val']:.1f}, ICT {p['ict_val']:.1f}); fixtures: {fix_str}; chance {p['chance']}%."
                print(
                    f"- {p['web_name']} ({team_name}, £{price:.1f}, Pos: {positions[pos]}) - Expected: {p['expected_points']:.2f} - Reason: {reason}")
                if p == best_captain:
                    print("  (Captain - Double points potential)")
                elif p == best_vice:
                    print("  (Vice-Captain)")

    print("\nBench (in priority order):")
    for p in bench:
        price = p['now_cost'] / 10.0
        team_name = get_team_name(teams, p['team'])
        pos_name = positions[p['element_type']]
        fix_str = ', '.join(
            f"vs {get_team_name(teams, f['opp_id'])} ({'H' if f['home'] else 'A'}, diff {f['diff']})" for f in
            p['fixtures'])
        reason = f"Backup with {p['expected_points']} pts potential (form {p['form_val']:.1f}, PPG {p['ppg_val']:.1f}); fixtures: {fix_str}; chance {p['chance']}%."
        print(
            f"- {p['web_name']} ({team_name}, £{price:.1f}, Pos: {pos_name}) - Expected: {p['expected_points']:.2f} - Reason: {reason}")


def main():
    """Main function to run FPL optimizer."""
    parser = argparse.ArgumentParser(description="FPL Team Optimizer")
    parser.add_argument('--squad', type=str, help='Current squad JSON file (overrides default)')
    parser.add_argument('--chip', choices=['wildcard', 'free_hit', 'bench_boost', 'triple_captain'], help='Chip to use')
    parser.add_argument('--transfers', type=int, default=1, help='Number of free transfers (1-5)')
    args = parser.parse_args()

    # Load or initialize current squad
    current_squad = load_squad() if not args.squad else []
    try:
        if args.squad:
            with open(args.squad, 'r') as f:
                current_squad = json.load(f)
    except Exception as e:
        logging.error(f"Error loading squad from {args.squad}: {e}")
        current_squad = []

    # Fetch data
    players, teams, events, fixtures, next_gw = fetch_data()

    # Adjust free transfers (AFCON rule for GW16)
    free_transfers = 5 if next_gw == 16 else min(args.transfers, 5)

    # Process data and optimize
    team_fixtures = calculate_team_fixtures(teams, fixtures)
    expected_points = calculate_expected_points(players, teams, team_fixtures)
    selected, total_cost, squad_expected = optimize_squad(players, expected_points, current_squad, free_transfers,
                                                          args.chip)
    best_lineup, best_form, best_proj, best_captain, best_vice = optimize_lineup(selected, expected_points, args.chip)
    bench = sorted([p for p in selected if p not in best_lineup], key=lambda p: p['expected_points'], reverse=True)

    # Save new squad
    save_squad(selected)

    print_output(next_gw, selected, total_cost, squad_expected, best_lineup, best_form, best_proj, best_captain,
                 best_vice, bench, teams, args.chip)


if __name__ == "__main__":
    main()