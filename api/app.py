# Imports
from flask import Flask, Response
from dotenv import load_dotenv
import os
from pymongo import MongoClient

# Setup
load_dotenv()
app = Flask(__name__)

# Globals
CRON_SECRET = os.getenv('CRON_SECRET')


@app.route('/')
async def root() -> Response:
    """
    """
    return {"msg", "Hello!"}, 200