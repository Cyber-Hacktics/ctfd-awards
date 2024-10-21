import zipfile
import pandas as pd
import json

def extract_first_blood(zip_file_path, output_file='first_blood_winners.json'):
    # Extract the zip file contents
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        extracted_folder = zip_file_path.replace('.zip', '')
        zip_ref.extractall(extracted_folder)

    # Load necessary JSON files
    with open(f'{extracted_folder}/db/solves.json', 'r') as file:
        solves_data = json.load(file)['results']

    with open(f'{extracted_folder}/db/submissions.json', 'r') as file:
        submissions_data = json.load(file)['results']

    with open(f'{extracted_folder}/db/challenges.json', 'r') as file:
        challenges_data = json.load(file)['results']

    with open(f'{extracted_folder}/db/teams.json', 'r') as file:
        teams_data = json.load(file)['results']

    with open(f'{extracted_folder}/db/users.json', 'r') as file:
        users_data = json.load(file)['results']

    # Convert to DataFrames
    submissions_df = pd.DataFrame(submissions_data)
    challenges_df = pd.DataFrame(challenges_data)
    teams_df = pd.DataFrame(teams_data)
    users_df = pd.DataFrame(users_data)

    # Filter out banned and hidden users, and banned teams
    invalid_users = users_df[(users_df['banned'] == True) | (users_df['hidden'] == True)]['id'].tolist()
    banned_teams = teams_df[teams_df['banned'] == True]['id'].tolist()

    # Filter for correct submissions, sort by date, and exclude invalid users/teams
    correct_submissions = submissions_df[
        (submissions_df['type'] == 'correct') & 
        (~submissions_df['user_id'].isin(invalid_users)) & 
        (~submissions_df['team_id'].isin(banned_teams))
    ].sort_values(by='date')

    # Find the first valid solve for each challenge
    first_solves = correct_submissions.groupby('challenge_id').first().reset_index()

    # Merge to get challenge names and team information
    first_solves = first_solves.merge(challenges_df[['id', 'name']], left_on='challenge_id', right_on='id', suffixes=('', '_challenge'))
    first_solves = first_solves.merge(teams_df[['id', 'name']], left_on='team_id', right_on='id', suffixes=('', '_team'))

    # Prepare the output for the "First Blood" award
    first_solves_awards = first_solves[['name', 'name_team', 'team_id']].copy()
    first_solves_awards.columns = ['challenge_name', 'team_name', 'team_id']
    first_solves_awards['team_members'] = first_solves_awards['team_id'].apply(
        lambda team_id: users_df[(users_df['team_id'] == team_id) & 
                                 (users_df['banned'] == False) & 
                                 (users_df['hidden'] == False)]['name'].tolist()
    )

    # Convert the results to a list of dictionaries
    results = first_solves_awards[['challenge_name', 'team_name', 'team_members']].to_dict(orient='records')

    # Write the results to a JSON file
    with open(output_file, 'w') as json_file:
        json.dump(results, json_file, indent=4)

    print(f"First Blood results saved to {output_file}")

# Example usage
zip_file_path = 'ctfd_export.zip'  # Replace with the actual file path
extract_first_blood(zip_file_path, output_file='first_blood_winners.json')