# ChatTB

DSCI 551 ChatDB Project Repo

Webpage: https://chat-tb.streamlit.app/

NOTE: The free postgres cloud database used for this website expired on 12/7. You can set up this repo locally with your own databases by following the instructions below.

## Codebase Descriptions
* app.py: Code for UI
* pattern_match.py: Code for translating natural language statement into query statement (both SQL and MongoDB)
* query_execution: Helper functions for communicating with the databases and generating example queries

## Instructions for running locally:
1. Clone the repo locally
2. Install requirements from **requirements.txt**
3. Create a directory in the project root called `.streamlit`
4. Create a file: `.streamlit/secrets.toml` and add API keys to your databases. The key for the sql database should be named `DATABASE_URL` and `MONGO_URL` for mongodb. _These URIs will be used to create the sqlalchemy engine and pymongo client._
5. In the command line, run: `streamlit run app.py`

## Database setup help
To set up a postgres database, navigate to https://render.com/ and create a new PostgreSQL database. Then, copy the external database url and paste it into the secrets file for `DATABASE_URL`. This database will be valid for 30 days.

To set up a mongodb database, navigate to https://www.mongodb.com/ and create a new database. To get the API key, go to (your cluster) > Connect > MongoDB for VSCode. Copy and paste the URL into the secrets file for `MONGO_URL`. Don't forget to replace the password in the URL (including the \<\> signs). If you forget the password, it can be found under Security > Database Access.
