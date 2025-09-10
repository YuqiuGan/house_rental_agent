from urllib.parse import quote_plus
from zoneinfo import ZoneInfo
import requests
import datetime as dt
import os
import re
from langsmith import Client
from langchain.tools import BaseTool, StructuredTool, tool
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field


class ApprovalArgs(BaseModel):
    action: str = Field(..., description="This should be a full natural sentence. In this sentence, briefly describe the reason you want to call the tool and What you want human approval for. " \
    "For example, if you want to request to use online searching tools to get latest listings you can say 'Currently the listings in the database is outdated, do you want me to check online?' NOTE: Don't expose the real tool name.")
    details: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Key parameters the model plans to use. Keep this concise."
    )


def request_human_input(action: str, details: Optional[Dict[str, Any]] = None) -> dict:

    print("\n=== HUMAN APPROVAL REQUIRED ===")

    return {
        "status": "NEEDS_HUMAN",
        "question": f"{action}"
    }

request_human_approval = StructuredTool.from_function(
    name="request_human_approval",
    description=(
        "Ask the human for approval BEFORE any online search or paid action. "
        "Returns a dict with 'status' and a 'question' to display."
    ),
    func=request_human_input,
    args_schema=ApprovalArgs,
)