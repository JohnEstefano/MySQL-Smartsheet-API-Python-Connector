# DB Cred
DB_HOST="Your host"
DB_USER="Your user"
DB_PASSWORD="Your password"
DB_NAME="Your DB name"

# Query (However many you need for your use case)
QUERY_1 = "Your query"

QUERY_2 = "Your query"

# Smartsheet
SMARTSHEET_API_TOKEN = 'Your token'
SMARTSHEET_SHEET_ID = 'Your sheet id: THIS ONE WITHOUT QUOTES'
CAMPAIGN_KEY_FORMULA = '=[Final Plan]@row + "_" + [Final Program]@row + "_" + Tactic@row + "_" + [Placement Start Date]@row'
FINAL_PLAN_FORMULA = '=VLOOKUP([IBM Plan]@row, {Tracker Plan/Program Match Key Range 1}, 2, false)'
FINAL_PROGRAM_FORMULA = '=VLOOKUP([IBM Program]@row, {Tracker Plan/Program Match Key Range 2}, 2, 0)'