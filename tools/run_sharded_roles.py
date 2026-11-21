#!/usr/bin/env python3
"""
Multi-Role Set Shard Orchestrator & Runner
=========================================
Supports sharding for:
  1. AI & Automation Engineering (15 roles, 3-5 safe shards)
  2. Supply Chain & Procurement (79 roles, 8 safe shards)
  3. Combined Master List (94 roles, 10 safe shards)

Usage:
  # View AI & Automation shards (3 shards of 5 roles each):
  python3 run_sharded_roles.py --role-set ai_automation --list-shards

  # View Supply Chain shards (8 shards):
  python3 run_sharded_roles.py --role-set supply_chain --list-shards

  # View Combined (All 94 roles across 10 shards):
  python3 run_sharded_roles.py --role-set all --list-shards

  # Run Shard 1 on LinkedIn:
  python3 run_sharded_roles.py --role-set ai_automation --platform linkedin --shard 1 --post-time 2week

  # Run Shard 1 on both platforms:
  python3 run_sharded_roles.py --role-set ai_automation --platform both --shard 1 --post-time 2week
"""

import sys
import os
import argparse
import subprocess
from pathlib import Path

SUPPLY_CHAIN_ROLES = [
    "Senior Commodity Manager",
    "Commodity Manager",
    "Commodity Buyer",
    "Commodity Lead",
    "Purchasing Commodity Manager",
    "Senior Commodity Buyer",
    "Global Supply Chain Analyst",
    "Global Supply Chain Specialist",
    "Global Supply Chain Lead",
    "Global Sourcing Specialist",
    "Global Commodity Manager",
    "Global Supply Chain Manager",
    "Commodity Specialist",
    "Procurement Manager",
    "Procurement Specialist",
    "Senior Procurement Specialist",
    "Senior Procurement Manager",
    "Assistant Procurement Manager",
    "Procurement Associate",
    "Senior Procurement Analyst",
    "Senior Procurement Officer",
    "Senior Procurement Engineer",
    "Senior Procurement Consultant",
    "Head of Procurement",
    "Procurement Officer",
    "Procurement Coordinator",
    "Procurement Engineer",
    "Procurement Assistant",
    "Sourcing Manager",
    "Sourcing Specialist",
    "Strategic Sourcing Manager",
    "Senior Manager Strategic Sourcing",
    "Senior Sourcing Analyst",
    "Senior Sourcing Manager",
    "Senior Sourcing Specialist",
    "Assistant Sourcing Manager",
    "Global Sourcing Lead",
    "Senior Manager Global Sourcing",
    "Strategic Sourcing Specialist",
    "Sourcing Lead",
    "Buyer",
    "Senior Buyer",
    "Purchasing Manager",
    "Purchasing Representative",
    "Procurement Buyer",
    "Commodity Analyst",
    "Senior Purchasing Agent",
    "Senior Purchasing Specialist",
    "Assistant Purchasing Agent",
    "Purchasing Assistant",
    "Senior Purchasing Analyst",
    "Senior Purchasing Officer",
    "Purchasing Coordinator",
    "Associate Buyer",
    "Senior Purchasing Assistant",
    "Purchasing Supervisor",
    "Purchase Specialist",
    "Junior Buyer",
    "Purchasing Officer",
    "Senior Purchasing Manager",
    "Purchasing Analyst",
    "Purchasing Buyer",
    "Materials Buyer",
    "Assistant Buyer",
    "Assistant Purchasing Manager",
    "Purchasing Consultant",
    "Purchasing Associate",
    "Purchasing Agent",
    "Director of Procurement",
    "Procurement Analyst",
    "Commodity Trader",
    "Director of Purchasing",
    "Category Buyer",
    "Strategic Sourcing Analyst",
    "Commodity Broker",
    "Sourcing Associate",
    "Strategic Sourcing Lead",
    "Sourcing Director",
    "Global Sourcing Manager",
]

AI_AUTOMATION_ROLES = [
    "AI & ML Application Development",
    "Generative AI Implementation",
    "AI Chatbot Development",
    "Python Development & Automation",
    "LLM Integration (OpenAI, Gemini, Claude)",
    "AI Agent Development",
    "RAG-Based Application Development",
    "Business Process Automation",
    "API Development & Integration",
    "Workflow Automation (n8n)",
    "Web Scraping & Data Automation",
    "Database Integration",
    "AI Solution Testing & Deployment",
    "Backend Development using FastAPI",
    "AI Automation engineer",
]

ROLE_SETS = {
    "ai_automation": {
        "name": "AI & Automation Engineering",
        "default_shards": 3,
        "roles": AI_AUTOMATION_ROLES,
    },
    "supply_chain": {
        "name": "Supply Chain & Procurement",
        "default_shards": 8,
        "roles": SUPPLY_CHAIN_ROLES,
    },
    "all": {
        "name": "All Combined Roles (Supply Chain + AI)",
        "default_shards": 10,
        "roles": SUPPLY_CHAIN_ROLES + AI_AUTOMATION_ROLES,
    },
}

def get_shard_range(role_list: list[str], shard_num: int, total_shards: int) -> tuple[int, int, list[str]]:
    total_roles = len(role_list)
    chunk_size = (total_roles + total_shards - 1) // total_shards
    start_idx = (shard_num - 1) * chunk_size + 1
    end_idx = min(shard_num * chunk_size, total_roles)
    shard_roles = role_list[start_idx - 1:end_idx] if start_idx <= total_roles else []
    return start_idx, end_idx, shard_roles

def list_shards(role_set_key: str, total_shards: int):
    info = ROLE_SETS[role_set_key]
    role_list = info["roles"]
    print(f"\n========================================================")
    print(f" {info['name'].upper()} — {len(role_list)} ROLES ({total_shards} SHARDS)")
    print(f"========================================================")
    for s in range(1, total_shards + 1):
        start, end, roles = get_shard_range(role_list, s, total_shards)
        print(f"\n🔹 Shard {s}/{total_shards} [Roles {start} - {end}] ({len(roles)} roles):")
        for r in roles:
            print(f"    • {r}")
    print(f"\n========================================================\n")

def run_scraper_for_roles(script_name: str, roles: list[str], extra_args: list[str]):
    cur = Path(__file__).resolve().parent
    root = cur.parent if cur.name in ("tools", "scrapers") else cur
    candidates = [
        root / "scrapers" / "linkedin" / script_name,
        root / "scrapers" / "indeed" / script_name,
        root / "linkedin_scrapers" / script_name,
        root / "indeed_scrapers" / script_name,
        root / script_name,
        cur / script_name,
        cur / "linkedin_scrapers" / script_name,
        cur / "indeed_scrapers" / script_name,
        cur / "scrapers" / "linkedin" / script_name,
        cur / "scrapers" / "indeed" / script_name,
        Path.cwd() / "scrapers" / "linkedin" / script_name,
        Path.cwd() / "scrapers" / "indeed" / script_name,
        Path.cwd() / script_name,
    ]
    script_path = None
    for c in candidates:
        if c.exists():
            script_path = c
            break

    if not script_path:
        print(f"❌ Error: Could not locate {script_name}")
        print("   Searched:")
        for c in candidates:
            print(f"     - {c}")
        sys.exit(1)

    script_dir = str(script_path.parent)
    env = os.environ.copy()
    current_pp = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = f"{script_dir}:{current_pp}" if current_pp else script_dir

    for idx, role in enumerate(roles, 1):
        cmd = [
            sys.executable,
            str(script_path),
            "--role", role,
        ] + extra_args

        print(f"\n🚀 [{idx}/{len(roles)}] Running: {' '.join(cmd)}")
        result = subprocess.run(cmd, env=env, cwd=script_dir)
        if result.returncode != 0:
            print(f"⚠️ Warning: {script_name} returned code {result.returncode} for role '{role}'")

def main():
    parser = argparse.ArgumentParser(description="Multi-Role Set Shard Orchestrator")
    parser.add_argument("--role-set", choices=["ai_automation", "supply_chain", "all"], default="ai_automation",
                        help="Role set to scrape (default: ai_automation)")
    parser.add_argument("--platform", choices=["linkedin", "indeed", "both"], default="both",
                        help="Platform to scrape (default: both)")
    parser.add_argument("--shard", type=int, default=1,
                        help="Shard number to execute (1-based, default: 1)")
    parser.add_argument("--total-shards", type=int, default=None,
                        help="Total number of shards (defaults: 3 for AI, 8 for Supply Chain, 10 for All)")
    parser.add_argument("--post-time", choices=["2weeks", "week", "day", "month", "any", "2week"], default="2weeks",
                        help="Job post time filter (default: 2weeks)")
    parser.add_argument("--location", default="India",
                        help="Target search location (default: India)")
    parser.add_argument("--country", default="india",
                        help="Indeed country code (default: india)")
    parser.add_argument("--no-state-split", "--no-split", action="store_true",
                        help="Disable auto state splitting")
    parser.add_argument("--proxy", default=None,
                        help="HTTP/HTTPS proxy URL (or set PROXY_URL / RESIDENTIAL_PROXY env var)")
    parser.add_argument("--list-shards", action="store_true",
                        help="Display shard breakdown without running")

    args, unknown = parser.parse_known_args()

    role_info = ROLE_SETS[args.role_set]
    total_shards = args.total_shards or role_info["default_shards"]

    if args.list_shards:
        list_shards(args.role_set, total_shards)
        return

    if args.shard < 1 or args.shard > total_shards:
        print(f"❌ Error: --shard must be between 1 and {total_shards}")
        sys.exit(1)

    start_idx, end_idx, roles = get_shard_range(role_info["roles"], args.shard, total_shards)
    print(f"✅ Selected Role Set: {role_info['name']}")
    print(f"   Shard {args.shard}/{total_shards}: Roles {start_idx} to {end_idx} ({len(roles)} roles)")
    for r in roles:
        print(f"   - {r}")

    extra_args = ["--post-time", args.post_time]
    if args.location:
        extra_args += ["--location", args.location]
    if args.no_state_split:
        extra_args += ["--no-state-split"]
    proxy_val = args.proxy or os.getenv("RESIDENTIAL_PROXY") or os.getenv("PROXY_URL")
    if proxy_val:
        os.environ["PROXY_URL"] = proxy_val
        extra_args += ["--proxy", proxy_val]
    extra_args += unknown

    if args.platform in ("linkedin", "both"):
        print(f"\n--- [Shard {args.shard}] Starting LinkedIn Scraper ---")
        run_scraper_for_roles("linkedin_68_roles.py", roles, extra_args)

    if args.platform in ("indeed", "both"):
        print(f"\n--- [Shard {args.shard}] Starting Indeed Scraper ---")
        extra_args_indeed = list(extra_args)
        if args.country:
            extra_args_indeed += ["--country", args.country]
        run_scraper_for_roles("indeed_68_roles.py", roles, extra_args_indeed)

if __name__ == "__main__":
    main()
