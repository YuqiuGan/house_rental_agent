from urllib.parse import quote_plus
from zoneinfo import ZoneInfo
import requests
import datetime as dt
import os
import re
from langsmith import Client
from langchain.tools import BaseTool, StructuredTool, tool

@tool
def get_date_time_now():

    """
    This function helps agent to know what's the date time now.
    """
    return dt.datetime.now()